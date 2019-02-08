import argparse
import logging
import json
import os
import random
import time
import unicodedata

from tempfile import TemporaryDirectory
import xml.etree.ElementTree as ET

import requests

import numpy as np

import mercantile
import rasterio

from google.cloud import storage

from rasterio.vrt import WarpedVRT

from skimage import io
from skimage import exposure
from skimage import transform

import twitter


logging.basicConfig(
    level=logging.INFO,
    datefmt="%X",
    format="%(asctime)s %(levelname)-8s %(message)s",
)

storage_client = storage.Client()
bucket_name = "gcp-public-data-sentinel-2"


def twitter_credentials():
    return dict(
        consumer_key=os.getenv("CONSUMER_KEY"),
        consumer_secret=os.getenv("CONSUMER_SECRET"),
        access_token_key=os.getenv("ACCESS_TOKEN_KEY"),
        access_token_secret=os.getenv("ACCESS_TOKEN_SECRET"),
    )


def count_pixels(img, colour=[0.0, 0.0, 0.0]):
    """Count pixels that are specified colour"""
    return np.sum(
        np.logical_and(
            img[:, :, 0] == colour[0],
            img[:, :, 1] == colour[1],
            img[:, :, 2] == colour[2],
        )
    )


def random_mgrs(seed=657):
    rng = random.Random(seed)
    gzd = rng.choice(range(1, 61))
    sqid = rng.choice("CDEFGHJKLMNPQRSTUVWX")
    col = rng.choice("ABCDEFGHJKLMNPQRSTUVWXYZ")
    row = rng.choice("ABCDEFGHJKLMNPQRSTUV")
    return (gzd, sqid, "%s%s" % (col, row))


def get_address(lat, lng):
    """Convert latitude and longitude into an address using OSM"""

    def _norm_len(s):
        return len(unicodedata.normalize("NFC", s).encode("utf-8"))

    def _cut(s, max_len=72):
        if _norm_len(s) < max_len:
            return s
        while _norm_len(s) >= max_len:
            ss = s.split(",")
            s = ", ".join([x.strip() for x in ss[1:]])
        return s

    # otherwise we get unicode mixed with latin which often exceeds
    # the 140character limit of twitter :(
    headers = {"Accept-Language": "en-US,en;q=0.8"}
    nominatim_url = (
        "http://nominatim.openstreetmap.org/reverse?lat=%f&lon=%f&"
        "addressdetails=0&format=json&zoom=6&extratags=0"
    )
    info = json.loads(
        requests.get(nominatim_url % (lat, lng), headers=headers).text
    )
    if "error" in info:
        return "Unknown location, do you recognise it?"

    return _cut(info["display_name"])


def format_lat_lng(lat, lng):
    s = ""
    if lat < 0:
        s += "%.1f°S" % abs(lat)
    else:
        s += "%.1f°N" % abs(lat)

    s += " "

    if lng < 0:
        s += "%.1f°W" % abs(lng)
    else:
        s += "%.1f°E" % abs(lng)

    return s


def pick_date(area=(32, "T", "MT"), satellite="A", skip=0):
    satellite = "/S2%s_" % satellite

    bucket = storage_client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix="tiles/%i/%s/%s/" % area))
    if not blobs:
        return None

    blobs = [b for b in blobs if satellite in b.name]

    band2s = [b for b in blobs if b.name.endswith("B02.jp2")]
    # go up a few levels to find the meta data XML file
    cloud_meta = [
        "/".join(c.name.split("/")[:-4] + ["MTD_MSIL1C.xml"]) for c in band2s
    ]

    cloud_free = []
    for band, cloud in zip(reversed(band2s), reversed(cloud_meta)):
        meta_blob = bucket.blob(cloud)
        try:
            xml = ET.fromstring(meta_blob.download_as_string())
        except Exception:
            continue

        cloud_cover = float(next(xml.iter("Cloud_Coverage_Assessment")).text)
        if cloud_cover > 70:
            continue

        logging.info(
            "Picked %s with cloud coverage of %i%%."
            % (cloud.rsplit("/", 1)[0], cloud_cover)
        )

        cloud_free.append(band.name.replace("_B02.jp2", "_B0%i.jp2"))

        # only go back far enough to be able to fullfill skip request
        if len(cloud_free) > skip:
            break

    if not cloud_free:
        return None

    # skip as many as possible, default to last available
    return cloud_free[max(-len(cloud_free), -skip)]


def sentinel2_bot(
    seed=None, post=True, loop=False, clean_up=False, period=60 * 60, mgrs=None
):
    last_post = time.time() - period

    rng = random.Random(seed)
    seed = rng.randint(1, 2 ** 64)

    if mgrs is None:
        mgrs_ = random_mgrs(seed=seed)
    else:
        mgrs_ = mgrs

    forever = True
    while forever:
        picked = None
        while picked is None:
            seed += 1
            mgrs_ = random_mgrs(seed=seed)
            logging.info("Trying MGRS: %s" % (mgrs_,))
            picked = pick_date(area=mgrs_)  # , skip=2)

        bucket = storage_client.get_bucket(bucket_name)

        bands = []
        transformed_bands = []
        for band in (4, 3, 2):
            blob = bucket.blob(picked % band)

            with TemporaryDirectory() as d:
                b3 = os.path.join(d, "b.jp2")
                blob.download_to_filename(b3)
                with rasterio.open(b3) as src:
                    lng, lat = src.lnglat()
                    logging.info("Coordinate of the tile: %f, %f" % (lat, lng))
                    tile = mercantile.tile(lng, lat, 10)
                    merc_bounds = mercantile.xy_bounds(tile)
                    with WarpedVRT(src, dst_crs="epsg:3857") as vrt:
                        window = vrt.window(*merc_bounds)
                        arr_transform = vrt.window_transform(window)
                        arr = vrt.read(window=window)
                        bands.append(arr)
                        transformed_bands.append(arr_transform)

        logging.info("Address: %s" % get_address(lat, lng))

        # normal window
        rgb = np.stack([a.squeeze(0) for a in bands])

        rgb = np.moveaxis(rgb, 0, -1)
        logging.info("Image dimensions %s." % (rgb.shape,))

        # count fraction of exactly black pixels, this happens with
        # partial acquisitions
        black = count_pixels(rgb)
        print(black, rgb.shape[0], rgb.shape[1])
        if black / (rgb.shape[0] * rgb.shape[1]) > 0.3:
            logging.info("Skipping image because it is incomplete.")
            continue

        low, high = np.percentile(rgb, (1, 97))
        rgb = exposure.rescale_intensity(rgb, in_range=(low, high))
        if exposure.is_low_contrast(rgb):
            logging.info("Skipping image because it is low contrast")
            continue

        fname = picked.split("/")[-1]
        identifier, _ = fname.rsplit("_", 1)
        fname = "/tmp/%s_rgb.jpg" % identifier
        fname_small = "/tmp/%s_rgb_small.jpg" % identifier

        io.imsave(fname, rgb, quality=90)
        io.imsave(
            fname_small,
            transform.resize(rgb, (1098 * 2, 1098 * 2)),
            quality=90,
        )

        date = identifier[7:-7]
        day = date[-2:]
        month = date[-4:-2]
        year = date[:4]

        MSG = "{location} ({lat_lng}), {date}"
        msg = MSG.format(
            date="%s %s %s" % (day, month, year),
            lat_lng=format_lat_lng(lat, lng),
            location=get_address(lat, lng),
        )
        logging.info("Twitter message: %s" % msg)
        delta = period - (time.time() - last_post)
        if delta > 0.0:
            logging.info("Sleeping for %is before posting." % delta)
            time.sleep(delta)

        if post:
            twitter_api = twitter.Api(**twitter_credentials())
            logging.info("Posting to twitter.")
            twitter_api.PostUpdate(
                msg,
                media=fname_small,
                latitude=lat,
                longitude=lng,
                display_coordinates=True,
            )

        if clean_up:
            logging.info("Removing image %s." % fname)
            os.remove(fname)
            logging.info("Removing image %s." % fname_small)
            os.remove(fname_small)

        if not loop:
            forever = False


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "--seed",
        help="Seed for random number generator",
        default=random.randint(1, 2 ** 64),
    )
    argparser.add_argument(
        "--post", help="Post to twitter", action="store_true"
    )
    argparser.add_argument("--loop", help="Loop forever", action="store_true")
    argparser.add_argument(
        "--period",
        help="Minimum delay between imaging loops in seconds",
        type=int,
        default=60 * 60,
    )
    argparser.add_argument("--mgrs", help="MGRS to use")
    args = argparser.parse_args()

    if args.mgrs:
        loop = False
    else:
        loop = args.loop

    if args.mgrs:
        mgrs = args.mgrs.split("/")
        mgrs = int(mgrs[0]), mgrs[1], mgrs[2]
    else:
        mgrs = None

    sentinel2_bot(
        seed=args.seed,
        post=args.post,
        loop=loop,
        period=args.period,
        mgrs=mgrs,
    )

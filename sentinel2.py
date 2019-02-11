import argparse
import logging
import json
import os
import random
import time
import unicodedata

from functools import lru_cache
from tempfile import TemporaryDirectory
import xml.etree.ElementTree as ET

import requests

import numpy as np

import rasterio

from google.cloud import storage

from rasterio.windows import Window

from skimage import io
from skimage import exposure
from skimage import transform

import twitter

import matplotlib

matplotlib.use("TkAgg")
import matplotlib.pyplot as plt


logging.basicConfig(
    level=logging.INFO,
    datefmt="%X",
    format="%(asctime)s %(levelname)-8s %(message)s",
)

storage_client = storage.Client()
bucket_name = "gcp-public-data-sentinel-2"
BUCKET = storage_client.get_bucket(bucket_name)

HERE = os.path.dirname(os.path.abspath(__file__))
VALID_MGRS = []
with open(os.path.join(HERE, "valid_mgrs")) as f:
    for mgrs in f:
        VALID_MGRS.append((int(mgrs[:2]), mgrs[2:3], mgrs[3:5]))

MONTHS = [
    "Padding to make the indexing right",
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]


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
    return rng.choice(VALID_MGRS)


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


@lru_cache(maxsize=256)
def list_blobs(params):
    while True:
        try:
            return list(
                BUCKET.list_blobs(prefix="tiles/%i/%s/%s/S2%s_MSIL1C" % params)
            )
        except Exception:
            logging.info("Sleeping for 5s")
            time.sleep(5)


def pick_date(area=(32, "T", "MT"), satellite="A", skip=0):
    params = area + (satellite,)
    blobs = list_blobs(params)
    if not blobs:
        logging.info("No blobs for MGRS: %s" % (area,))
        return None

    band2s = [b for b in blobs if b.name.endswith("B02.jp2")]
    # go up a few levels to find the meta data XML file
    cloud_meta = [
        "/".join(c.name.split("/")[:-4] + ["MTD_MSIL1C.xml"]) for c in band2s
    ]

    cloud_free = []
    for band, cloud in zip(reversed(band2s), reversed(cloud_meta)):
        meta_blob = BUCKET.blob(cloud)
        try:
            xml = ET.fromstring(meta_blob.download_as_string())
        except Exception:
            logging.info("Error parsing metadata XML. Sleep 2s.")
            time.sleep(2)
            continue

        cloud_cover = float(next(xml.iter("Cloud_Coverage_Assessment")).text)
        if cloud_cover > 70:
            logging.info("Skipping because of cloud coverage.")
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
    return cloud_free[min(len(cloud_free), skip)]


def sentinel2_bot(
    seed=None,
    post=True,
    loop=False,
    clean_up=False,
    period=60 * 60,
    mgrs=None,
    output="/tmp",
    skip=0,
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
        time.sleep(1.5)
        if mgrs is None:
            picked = None
            while picked is None:
                time.sleep(1.5)
                seed += 1
                mgrs_ = random_mgrs(seed=seed)
                logging.info("Trying MGRS: %s" % (mgrs_,))
                picked = pick_date(area=mgrs_, skip=skip)
        else:
            picked = pick_date(area=mgrs_, skip=skip)

        logging.info("Picked MGRS: %s" % (mgrs_,))

        bands = []
        for band in (4, 3, 2):
            blob = BUCKET.blob(picked % band)

            with TemporaryDirectory() as d:
                b3 = os.path.join(d, "b.jp2")
                blob.download_to_filename(b3)
                with rasterio.open(b3) as src:
                    lng, lat = src.lnglat()
                    logging.info("Coordinate of the tile: %f, %f" % (lat, lng))

                    bands.append(
                        src.read(window=Window(4392, 4392, 1098 * 2, 1098 * 2))
                    )

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

        # There should be no need to do weird things, just stretch each
        # band individually. This works! The fact that (deep) oceans end
        # up looking basically black makes sense because of how water reflects
        # or doesn't(!!) reflect normally incident light.
        for i in (0, 1, 2):
            low, high = np.percentile(rgb[:, :, i], (1, 99))
            rgb[:, :, i] = exposure.rescale_intensity(
                rgb[:, :, i], in_range=(low, high)
            )

        if exposure.is_low_contrast(rgb):
            logging.info("Skipping image because it is low contrast")
            continue

        if False:
            plt.hist(
                rgb[:, :, 0].ravel(),
                bins=256,
                color="r",
                range=(0, 2 ** 16),
                histtype="step",
                label="red",
            )
            plt.hist(
                rgb[:, :, 1].ravel(),
                bins=256,
                color="g",
                range=(0, 2 ** 16),
                histtype="step",
                label="green",
            )
            plt.hist(
                rgb[:, :, 2].ravel(),
                bins=256,
                color="b",
                range=(0, 2 ** 16),
                histtype="step",
                label="blue",
            )
            plt.legend(loc="best")
            plt.show()

        os.makedirs(output, exist_ok=True)

        fname = picked.split("/")[-1]
        identifier, _ = fname.rsplit("_", 1)
        fname = "%s/%s_rgb.jpg" % (output, identifier)
        fname_small = "%s/%s_rgb_small.jpg" % (output, identifier)

        io.imsave(fname, rgb, quality=90)
        io.imsave(
            fname_small,
            transform.resize(rgb, (1098 * 2, 1098 * 2)),
            quality=90,
        )

        date = identifier[7:-7]
        day = date[-2:]
        month = MONTHS[int(date[-4:-2])]
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
                media=[fname_small],
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

        last_post = time.time()


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
        "--clean-up", help="Clean up outputs", action="store_true"
    )
    argparser.add_argument("--output", help="Output directory", default="/tmp")
    argparser.add_argument(
        "--period",
        help="Minimum delay between imaging loops in seconds",
        type=int,
        default=60 * 60,
    )
    argparser.add_argument(
        "--skip", help="Skip this many images backwards", type=int, default=0
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
        output=args.output,
        clean_up=args.clean_up,
        skip=args.skip,
    )

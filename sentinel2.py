import calendar
import json
import math
import os
import random
import requests
import shutil
import time
import unicodedata
import urllib
import xml.etree.ElementTree as ET

import numpy as np

from skimage import novice
from skimage import io
from skimage import exposure

from rio_tiler import sentinel2

import twitter


BASE_URL = "http://sentinel-s2-l1c.s3.amazonaws.com/"


def random_mgrs(seed=657):
    rng = random.Random(seed)
    gzd = rng.choice(range(1, 61))
    sqid = rng.choice("CDEFGHJKLMNPQRSTUVWX")
    col = rng.choice("ABCDEFGHJKLMNPQRSTUVWXYZ")
    row = rng.choice("ABCDEFGHJKLMNPQRSTUV")
    return (gzd, sqid, "%s%s" % (col, row))


def get_listing(prefix):
    r = requests.get(BASE_URL + "?delimiter=/&prefix=%s" % prefix)
    text = r.text
    if 'CommonPrefixes' in text:
        root = ET.fromstring(r.text)
        prefixes = []
        for p in root.iter('{http://s3.amazonaws.com/doc/2006-03-01/}CommonPrefixes'):
            prefixes.append(p[0].text)
        return prefixes
    else:
        return []


def get_tileinfo(prefix):
    print(BASE_URL + prefix + 'tileInfo.json')
    r = requests.get(BASE_URL + prefix + 'tileInfo.json')
    return json.loads(r.text)


def not_nan(x):
    return x[~np.isnan(x)]


def image_interestingness(prefix):
    """Calculate how visually interesting a picture is."""
    try:
        img = novice.open(BASE_URL + prefix).xy_array
    except (requests.HTTPError, urllib.error.HTTPError):
        return -1.

    red = img[:,:,0].mean()
    green = img[:,:,1].mean()
    blue = img[:,:,2].mean()

    return (red + green) / 2 / blue


def get_position(flyby):
    """Extract coordinates of this image"""
    _, n, g, gg, year, month, day, i,_ = flyby.split("/")
    scene_id = "S2A_tile_%s%02i%02i_%02i%s%s_%s" % (year, int(month), int(day),
                                                    int(n), g, gg, i)
    #scene_id = 'S2A_tile_20171103_32TMT_0'
    bounds = sentinel2.bounds(scene_id)
    return centroid(bounds)


def get_address(lat, lng):
    """Convert latitude and longitude into an address using OSM"""
    def _norm_len(s):
        return len(unicodedata.normalize("NFC", s).encode('utf-8'))
    def _cut(s, max_len=72):
        if _norm_len(s) < max_len: return s
        while _norm_len(s) >= max_len:
            ss = s.split(",")
            s = ', '.join([x.strip() for x in ss[1:]])
        return s

    # otherwise we get unicode mixed with latin which often exceeds
    # the 140character limit of twitter :(
    headers = {'Accept-Language': "en-US,en;q=0.8"}
    nominatim_url = ("http://nominatim.openstreetmap.org/reverse?lat=%f&lon=%f&"
                     "addressdetails=0&format=json&zoom=6&extratags=0")
    info = json.loads(requests.get(nominatim_url % (lat, lng),
                                   headers=headers).text)
    if 'error' in info:
        return 'Unknown location, do you know it? Tell @openstreetmap'

    return _cut(info['display_name'])


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


def deg2num(lat_deg, lon_deg, zoom):
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return (xtile, ytile)


def centroid(bounds):
    bounds = bounds['bounds']
    lat = (bounds[1] + bounds[3]) / 2
    lng = (bounds[0] + bounds[2]) / 2
    return lat, lng


def process_image(flyby):
    directory_name = flyby.replace("/", "-")
    directory_name = '/tmp/%s' % directory_name
    output_image_fname = directory_name + "/B.jpg"

    # if the image already exists, there is nothing to do. This adds a simple
    # mechanism for caching results
    if os.path.exists(output_image_fname):
        return output_image_fname

    os.makedirs(directory_name)

    z = 12
    _, n, g, gg, year, month, day, i, _ = flyby.split("/")
    scene_id = "S2A_tile_%s%02i%02i_%02i%s%s_%s" % (year, int(month), int(day),
                                                    int(n), g, gg, i)
    #scene_id = 'S2A_tile_20171103_32TMT_0'
    bounds = sentinel2.bounds(scene_id)
    x, y = deg2num(*centroid(bounds), z)
    #x, y = deg2num(20.820567, 92.367358, z)

    tile = sentinel2.tile(scene_id, x, y, z, tilesize=1098*2)

    tile = np.transpose(tile, (1, 2, 0))

    tile[0, :, :] = tile[0, :, :] * 0.93
    rgb = tile

    low, high = np.percentile(rgb, (1, 97))
    rgb = exposure.rescale_intensity(rgb, in_range=(low, high))

    io.imsave(output_image_fname, rgb, quality=90)

    return output_image_fname


def post_candidate(flyby, post=False, api=None):
    tile_info = get_tileinfo(flyby)

    lat, lng = get_position(flyby)

    coverage = float(tile_info.get('dataCoveragePercentage', 0.))
    complete = coverage > 99
    complete = coverage > 80
    cloudy_pixels = float(tile_info.get('cloudyPixelPercentage', 0.))
    cloudy = cloudy_pixels > 35.
    cloudy = cloudy_pixels > 135.
    interestingness = image_interestingness(flyby + "preview.jpg")

    MSG = "{location} ({lat_lng}), {date}"

    print('coverage:', coverage, 'clouds:', cloudy_pixels,
          '(R+G)/2/B %.1f' % interestingness)
    if (complete and not cloudy and (interestingness > 0.008)):
        print('Cloudy: %.2f Coverage: %.2f' % (cloudy_pixels, coverage))
        print('(R+G)/2/B %.1f' % interestingness)
        print(lat, lng, get_address(lat, lng))
        parts = flyby.split('/')
        day = parts[-3]
        month = calendar.month_name[int(parts[-4])]
        year = parts[-5]
        print(MSG.format(date="%s %s %s" %(day, month, year),
                         lat_lng=format_lat_lng(lat, lng),
                         location=get_address(lat, lng)))

        image_fname = process_image(flyby)

        print(image_fname)
        print("Good enough for government work.")

        if post:
            api.PostUpdate(MSG.format(date="%s %s %s" %(day, month, year),
                                      lat_lng=format_lat_lng(lat, lng),
                                      location=get_address(lat, lng)),
                           media=image_fname,
                           latitude=lat, longitude=lng,
                           display_coordinates=True,
                           )
            shutil.rmtree(os.path.dirname(image_fname), ignore_errors=True)

        return flyby

    return False


def random_candidate(max_retries=100, n_successes=None, seed=2,
                     post=False, api=None):
    """Pick random coordinates and check if there is an image there.

    Will guess up to `max_retries` coordinates and check if there
    is an image available for them. Will stop after posting the first
    image to twitter if `post=True` or once it has found `n_successes`.
    """
    rng = random.Random(seed)

    good_flybys = []
    for n in range(max_retries):
        url = "tiles/%s/%s/%s/" % random_mgrs(seed=rng.randint(1,2**64))
        years = get_listing(url)
        if years:
            year = rng.choice(years)
            months = get_listing(year)
            if months:
                month = rng.choice(months)
                days = get_listing(month)
                if days:
                    day = rng.choice(days)
                    flybys = get_listing(day)
                    flyby = rng.choice(flybys)

                    print("Iteration:", n, flyby)
                    print(BASE_URL + flyby + "preview.jpg")

                    try:
                        good_flyby = post_candidate(flyby, post=post, api=api)
                    except urllib.error.HTTPError:
                        time.sleep(1)
                        continue

                    if good_flyby:
                        # posting, so stop after one image
                        if post:
                            return None

                        # collecting/caching images
                        good_flybys.append(good_flyby)
                        if (n_successes is not None and
                            n_successes <= len(good_flybys)):
                            return good_flybys

                    time.sleep(0.5)


def twitter_credentials():
    return dict(consumer_key=os.getenv("CONSUMER_KEY"),
                consumer_secret=os.getenv("CONSUMER_SECRET"),
                access_token_key=os.getenv("ACCESS_TOKEN_KEY"),
                access_token_secret=os.getenv("ACCESS_TOKEN_SECRET"))


def loop(twitter, period=3600, seed=2):
    """Keep running for ever and ever and ever.

    Will post an image to twitter every `period` seconds.
    """
    # first find an image and process it. Then sleep till
    # it is time to post it, then look for the next image, then go to sleep,...
    # this way it should be easier to post on time
    rng = random.Random(seed)

    cached_flybys = random_candidate(max_retries=2000, n_successes=1,
                                     seed=rng.randint(1, 2**64))

    while True:
        flyby = cached_flybys.pop()
        good_flyby = post_candidate(flyby, post=True, api=twitter)
        last_post = time.time()

        cached_flybys += random_candidate(max_retries=2000, n_successes=1,
                                          seed=rng.randint(1, 2**64))

        time.sleep(period - (time.time() - last_post))


if __name__ == "__main__":
    import sys
    flyby = sys.argv[1]
    if len(sys.argv) == 3:
        seed = sys.argv[2]
    else:
        seed = random.randint(1,2**64)

    if flyby == 'random':
        random_candidate(seed=seed, max_retries=2000, n_successes=1)

    elif flyby == 'forever':
        api = twitter.Api(**twitter_credentials())
        loop(api, seed=seed)

    elif flyby == 'random-post':
        flybys = random_candidate(seed=seed, max_retries=2000, n_successes=1)
        api = twitter.Api(**twitter_credentials())
        post_candidate(flybys[0], api=api, post=True)

    else:
        api = twitter.Api(**twitter_credentials())
        post_candidate(flyby, api=api, post=True)

import calendar
import json
import os
import random
import requests
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET

import numpy as np

import pyproj

from skimage import novice
from skimage.feature import corner_harris, corner_subpix, corner_peaks

import twitter


BASE_URL = "http://sentinel-s2-l1c.s3.amazonaws.com/"


def get_bands(flyby):
    """Get bands 2, 3 and 4 from AWS for this flyby"""
    directory_name = flyby.replace("/", "-")
    directory_name = '/tmp/%s' % directory_name

    # if the directory exists we assume the download was a success
    if os.path.exists(directory_name):
        return directory_name

    os.makedirs(directory_name)

    for i in (2, 3, 4):
        r = requests.get(BASE_URL + flyby + "B0%i.jp2" % i, stream=True)
        path = os.path.join(directory_name, "B0%i.jp2" % i)
        if r.status_code == 200:
            with open(path, 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)

    return directory_name


def process_bands(directory_name):
    output_image_fname = os.path.join(directory_name, "B.jpg")
    if os.path.exists(output_image_fname):
        return output_image_fname

    fnames = [os.path.join(directory_name, x)
              for x in ("B04.jp2", "B03.jp2", "B02.jp2")]
    args = ["-combine", "-contrast-stretch", r"1%x3%", "+sigmoidal-contrast",
            r"5x50%", "-quality", "98", "-resize", r"10%",
            output_image_fname]
    cmd = ["convert"] + fnames + args
    subprocess.run(cmd, check=True)

    return output_image_fname


def random_mgrs(seed=657):
    rng = random.Random(seed)
    gzd = rng.choice(range(1,61))
    sqid = rng.choice("CDEFGHJKLMNPQRSTUVX")
    col = rng.choice("ABCDEFGHJKLMNPQRSTUVXYZ")
    row = rng.choice("ABCDEFGHJKLMNPQRSTUV")
    return (gzd, sqid, "%s%s"% (col, row))
    #return (50, 'M', 'LE')


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
    r = requests.get(BASE_URL + prefix + 'tileInfo.json')
    return json.loads(r.text)


def not_nan(x):
    return x[~np.isnan(x)]


def image_interestingness(prefix):
    """Calculate how visually interesting a picture is."""
    img = novice.open(BASE_URL + prefix)
    means = []
    stds = []
    corners = 0
    for i in (0,1,2):
        means.append(img.xy_array[:,:,i].mean())
        stds.append(img.xy_array[:,:,i].std())

        image = img.xy_array[:,:,i]
        coords = corner_peaks(corner_harris(image), min_distance=5)
        corners += not_nan(coords).shape[0]

    return (np.mean(means), np.mean(stds), corners)


def get_position(geometry):
    """Extract coordinates of this image"""
    coords = geometry['coordinates'][0]
    crs = int(geometry['crs']['properties']['name'].split(":")[-1])
    projection = pyproj.Proj(init='epsg:%i' % crs)
    coords = [(projection(*c, inverse=True)[1], projection(*c, inverse=True)[0])
              for c in coords]
    return np.mean(coords, axis=0)


def get_address(lat, lng):
    """Convert latitude and longitude into an address using OSM"""
    nominatim_url = ("http://nominatim.openstreetmap.org/reverse?lat=%f&lon=%f&"
                     "addressdetails=1&format=json&zoom=10&extratags=1")
    info = json.loads(requests.get(nominatim_url % (lat, lng)).text)
    if 'error' in info:
        return 'Unknown location, do you know it? Tell @openstreetmap'
    return info['display_name']


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


def post_candidate(flyby, post=False, api=None):
    tile_info = get_tileinfo(flyby)

    lat, lng = get_position(tile_info['tileGeometry'])

    coverage = float(tile_info['dataCoveragePercentage'])
    complete = coverage > 99
    cloudy_pixels = float(tile_info['cloudyPixelPercentage'])
    cloudy =  cloudy_pixels > 35.
    interestingness = image_interestingness(flyby + "preview.jpg")

    MSG = "{location} ({lat_lng}), {date}"

    print('coverage:', coverage, 'clouds:', cloudy_pixels)
    if (complete and not cloudy
        #and interestingness[0] > 60 and interestingness[1] > 20 and
        #interestingness[2] > 400
       ):
        print('Cloudy: %.2f Coverage: %.2f' % (cloudy_pixels, coverage))
        print('Mean %.1f +- %.2f Corners: %i' % interestingness)
        print(lat, lng, get_address(lat, lng))
        parts = flyby.split('/')
        day = parts[-3]
        month = calendar.month_name[int(parts[-4])]
        year = parts[-5]
        print(MSG.format(date="%s %s %s" %(day, month, year),
                         lat_lng=format_lat_lng(lat, lng),
                         location=get_address(lat, lng)))

        directory_name = get_bands(flyby)
        image_fname = process_bands(directory_name)

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

        return flyby

    return False


def random_candidate(max_retries=100, n_successes=1, seed=2,
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

                    good_flyby = post_candidate(flyby, post=post, api=api)

                    if good_flyby:
                        # posting, so stop after one image
                        if post:
                            return None

                        # collecting/caching images
                        good_flybys.append(good_flyby)
                        if n_successes <= len(good_flybys):
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
                                     seed=rng.randint(1,2**64))


    while True:
        flyby = cached_flybys.pop()
        good_flyby = post_candidate(flyby, post=True, api=twitter)
        last_post = time.time()

        cached_flybys += random_candidate(max_retries=2000, n_successes=1,
                                          seed=rng.randint(1,2**64))

        time.sleep(period - (time.time() - last_post))


if __name__ == "__main__":
    import sys
    flyby = sys.argv[1]

    if flyby == 'random':
        random_candidate()

    elif flyby == 'forever':
        seed = sys.argv[2]
        api = twitter.Api(**twitter_credentials())
        loop(api, seed=seed)

    else:
        api = twitter.Api(**twitter_credentials())
        post_candidate(flyby, api=api, post=True)

# posted at 20:14

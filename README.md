# Sentinel 2 Bot

I tweet pictures taken by the Sentinel 2 satellite.

Zahedan County, Sistan and Baluchestan Province, Iran (30.3°N 59.5°E), 30 August 2015:
<img src="https://pbs.twimg.com/media/CyHNOtBUoAEcd--.jpg" width="60%" />


# Sentinel 2 Satellite

Learn more about [Sentinel 2](http://www.esa.int/Our_Activities/Observing_the_Earth/Copernicus/Sentinel-2/Introducing_Sentinel-2) from
[ESA](http://esa.int) or on [wikipedia](https://en.wikipedia.org/wiki/Sentinel-2).


# Contributing

If you spot a bug or want to improve Sentinel 2 bot, make a pull request
or post an issue!


# License

The code is licensed under the MIT license. The images are Copernicus Sentinel data 2015-2016.


# Deploying

Brief guide on deploying Sentinel 2 bot:

* Create a twitter app, obtain credentials for that app and place them in
  `secrets.env`.

* Build the docker container with `docker build -t wildtreetech/sentinel2-bot .`

* Launch with: `docker run -d --env-file=secrets.env wildtreetech/sentinel2-bot python /tmp/sentinel2.py forever 333`

This will pick up the secrets for twitter from `secrets.env` and put the bot
into `forever` mode. It should post once an hour. The argument `333` is the
random seed used to pick locations. Every time you launch the bot you want to
change it. Otherwise it will post the same sequence of images as last time.

Currently the bot is deployed to [getcarina](https://getcarina.com) and
[@betatim](//twitter.com/betatim) has the twitter credentials. For
getcarina we need to use docker version 1.10.3.

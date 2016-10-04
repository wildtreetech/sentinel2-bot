FROM pritunl/archlinux:2016-10-01
MAINTAINER Tim Head <betatim@gmail.com>


RUN pacman --noconfirm -S gcc git imagemagick openjpeg2 python python-pip
# Add Tini
ENV TINI_VERSION v0.10.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN pip install matplotlib numpy pyproj scipy scikit-image

# use unreleased version until v3.3 is released
RUN pip install -e git+https://github.com/bear/python-twitter@d3eb170881b8fa81e06d6420b94d47131e5e9699#egg=python-twitter

ENV PYTHONIOENCODING utf8
ADD sentinel2.py /tmp/sentinel2.py

CMD ["python", "/tmp/sentinel2.py", "random"]

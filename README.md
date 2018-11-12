# littleDingo

## Build as littledingo
docker build -t littledingo .

## save image to tar file
docker save -o littledingo.tar littledingo

## scp to another docker environment
scp littledingo.tar xxx@xxx.xxx.xx.xx

# untar and save to image repository
docker load -i littledingo.tar

# run the image
docker run littledingo

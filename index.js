const AWS = require('aws-sdk');
const { spawn, spawnSync } = require('child_process');
const { createReadStream, createWriteStream } = require('fs');
var async = require('async');
const s3 = new AWS.S3();
var gm = require('gm')
            .subClass({ imageMagick: true }); // Enable ImageMagick integration.
const ffprobePath = '/opt/nodejs/ffprobe';
const ffmpegPath = '/opt/nodejs/ffmpeg';
const allowedTypes = ['mov', 'mpg', 'mpeg', 'mp4', 'wmv', 'avi', 'webm'];
const MAX_WIDTH = 380;
const MAX_HEIGHT = 380;

module.exports.handler = async (event, context, callback) => {
  
  // Object key may have spaces or unicode non-ASCII characters.
  const srcKey = decodeURIComponent(event.Records[0].s3.object.key).replace(/\+/g, ' ');
  const bucket = event.Records[0].s3.bucket.name;
  
  // Get Object metadata
  const target = s3.getSignedUrl('getObject', { Bucket: bucket, Key: srcKey, Expires: 1000 });
  var fileType = srcKey.match(/\.\w+$/);
 
  var dstBucket = bucket;
  var dstKey = srcKey + "_small";
  
  console.log(fileType);
  // Get Metadata and anaylze file type.
    
  if(srcKey.endsWith('_small')) {
      callback('Already Resized');
      return;
  }

  if (!fileType) {
    throw new Error(`invalid file type found for key: ${srcKey}`);
  }

  fileType = fileType[0].slice(1);
  var ffprobe;

  // for uploading an image
  if (allowedTypes.indexOf(fileType) === -1) {
    // Download the image from S3, transform, and upload to a different S3 bucket.
    async.waterfall([
        function download(next) {
            // Download the image from S3 into a buffer.
            s3.getObject({
                    Bucket: bucket,
                    Key: srcKey
                },
                next);
            },
        function transform(response, next) {
            gm(response.Body).size(function(err, size) {
                // Infer the scaling factor to avoid stretching the image unnaturally.
                var scalingFactor = Math.min(
                    MAX_WIDTH / size.width,
                    MAX_HEIGHT / size.height
                );
                var width  = scalingFactor * size.width;
                var height = scalingFactor * size.height;

                // Transform the image buffer in memory.
                this.resize(width, height)
                    .toBuffer(fileType, function(err, buffer) {
                        if (err) {
                            next(err);
                        } else {
                            next(null, response.ContentType, buffer);
                        }
                    });
            });
        },
        function upload(contentType, data, next) {
            // Stream the transformed image to a different S3 bucket.
            s3.putObject({
                    Bucket: dstBucket,
                    Key: dstKey,
                    Body: data,
                    ContentType: contentType,
                    ACL: "public-read"
                },
                next);
            }
        ], function (err) {
            if (err) {
                console.error(
                    'Unable to resize ' + bucket + '/' + srcKey +
                    ' and upload to ' + dstBucket + '/' + dstKey +
                    ' due to an error: ' + err
                );
            } else {
                console.log(
                    'Successfully resized ' + bucket + '/' + srcKey +
                    ' and uploaded to ' + dstBucket + '/' + dstKey
                );
            }

            callback(null, "message");
        }
    );
    
  } else { 
    ffprobe = spawnSync(ffprobePath, [
      '-v',
      'error',
      '-show_entries',
      'format=duration',
      '-of',
      'default=nw=1:nk=1',
      target
    ]);
    const duration = Math.ceil(ffprobe.stdout.toString());

    await createImage(duration * 0.25);
    await uploadToS3(1);
    await createImage(duration * .5);
    await uploadToS3(2);
    await createImage(duration * .75);
    await uploadToS3(3);
  
    return console.log(`processed ${bucket}/${srcKey} successfully`);
  }

  function createImage(seek) {
    return new Promise((resolve, reject) => {
      let tmpFile = createWriteStream(`/tmp/screenshot.jpg`);
      const ffmpeg = spawn(ffmpegPath, [
        '-ss',
        seek,
        '-i',
        target,
        '-vf',
        `thumbnail,scale=${MAX_WIDTH}:${MAX_HEIGHT}`,
        '-qscale:v',
        '2',
        '-frames:v',
        '1',
        '-f',
        'image2',
        '-c:v',
        'mjpeg',
        'pipe:1'
      ]);

      ffmpeg.stdout.pipe(tmpFile);

      ffmpeg.on('close', function(code) {
        tmpFile.end();
        resolve();
      });

      ffmpeg.on('error', function(err) {
        console.log(err);
        reject();
      });
    });
  }

  function uploadToS3(x) {
    return new Promise((resolve, reject) => {
      let tmpFile = createReadStream(`/tmp/screenshot.jpg`);
      let dstKey = srcKey+'_'+x+'_small';

      var params = {
        Bucket: bucket,
        Key: dstKey,
        Body: tmpFile,
        ContentType: `image/jpg`,
        ACL: "public-read"
      };

      s3.upload(params, function(err, data) {
        if (err) {
          console.log(err);
          reject();
        }
        console.log(`successful upload to ${bucket}/${dstKey}`);
        resolve();
      });
    });
  }

  

  
};

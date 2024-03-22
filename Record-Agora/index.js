const AWS = require('aws-sdk')
const fs = require("fs");
const { spawn, spawnSync, execSync } = require('child_process')
const { createReadStream, createWriteStream } = require('fs')
const https= require('https')
var shell = require('shelljs')

const s3 = new AWS.S3()
const ffprobePath = '/opt/nodejs/ffprobe'
const ffmpegPath = '/opt/nodejs/ffmpeg'
const allowedTypes = ['mov', 'mpg', 'mpeg', 'mp4', 'wmv', 'avi', 'webm']
const width = process.env.WIDTH
const height = process.env.HEIGHT
const bucket = process.env.BUCKET_NAME
const webhookDomain = process.env.WEBHOOK_DOMAIN
const webhookPath = process.env.WEBHOOK_PATH
const key = process.env.KEY
const efsPath = process.env.EFS_PATH

var globalPath = require('path')


function getRandomString(length) {
    var randomChars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var result = '';
    for ( var i = 0; i < length; i++ ) {
        result += randomChars.charAt(Math.floor(Math.random() * randomChars.length));
    }
    return result;
}

  function createImage(seek, target, pathThumbnailTmp) {
    return new Promise((resolve, reject) => {
      let tmpFile = createWriteStream(pathThumbnailTmp);
      const ffmpeg = spawn(ffmpegPath, [
        '-ss',
        seek,
        '-i',
        target,
        '-vf',
        `thumbnail,scale=${width}:${height}`,
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
        tmpFile.end()
        resolve()
      })

      ffmpeg.on('error', function(err) {
        console.log(err)
        reject()
      })
    })
  }

  function uploadToS3(dstKey, path, contentType = `image/jpg`) {
    var fileContent = fs.readFileSync(path);
    return new Promise((resolve, reject) => {
      var params = {
        Bucket: bucket,
        Key: dstKey,
        Body: fileContent,
        ContentType: contentType
      }

      s3.upload(params, function(err, data) {
        fs.unlinkSync(path);
        if (err) {
          console.log(err)
          reject()
        }
        console.log(`successful upload to ${bucket}/${dstKey}`)
        resolve()
      })
    })
  }
  
  function callWebhookSuccess(data) {
    return new Promise((resolve, reject) => {
      const options = {
        host: webhookDomain,
        port: 443,
        path: webhookPath,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      };
      const req = https.request(options, (res) => {
        var body = [];
        res.on('data', function(chunk) {
            body.push(chunk);
        });

        res.on('end', function() {
            try {
                body = JSON.parse(Buffer.concat(body).toString());
            } catch(e) {
                reject(e);
            }
            resolve(body);
        });
        
      });
      req.on('error', (e) => {
        reject(e.message);
      });
      req.write(JSON.stringify(data));
      req.end();
    });
  }
  

  function sizeOf(key) {
    return new Promise((resolve, reject) => {
      s3.headObject({ Key: key, Bucket: bucket }, (err, metadata) => {
       if (err && ['NotFound', 'Forbidden'].indexOf(err.code) > -1) return resolve();
       else if (err) {
        return reject(err);
       }
       return resolve(Number(metadata.ContentLength));
     });
        //.then(res => res.ContentLength);
    });
  }

  async function concatFiles(files) {
    let pathDir = `${efsPath}/${globalPath.dirname(files[0])}`;
    
    shell.mkdir('-p', pathDir);
    
    const listFilePath = pathDir + '/list.txt';
    let randomStr = getRandomString(60);
    let concatedFileLocal = `${pathDir}/concated_${randomStr}.mp4`;
    let concatedFileS3 = globalPath.dirname(files[0]) + `/final/calling_${randomStr}.mp4`;
    let outputPath = `${pathDir}/${randomStr}.mp4`;
    let content = '';

    if (files.length == 1) {
      const fileUrl = s3.getSignedUrl('getObject', { Bucket: bucket, Key: files[0], Expires: 1000 });
      await execSync(`${ffmpegPath} -ss 00:00:15 -i '${fileUrl}' -c copy ${outputPath}`);
      await uploadToS3(concatedFileS3, outputPath, `video/mp4`);
    } else {
      files.forEach(function (key, index) {
        content = content + "file '" + s3.getSignedUrl('getObject', { Bucket: bucket, Key: key, Expires: 1000 }) + "'" + '\r\n';
      });
      fs.writeFileSync(listFilePath, content);

      await execSync(`${ffmpegPath} -f concat -safe 0 -protocol_whitelist file,http,https,tcp,tls,crypto -i '${listFilePath}' -c copy ${concatedFileLocal}`);

      await execSync(`${ffmpegPath} -ss 00:00:15 -i '${concatedFileLocal}' -c copy ${outputPath}`);

      await uploadToS3(concatedFileS3, outputPath, `video/mp4`);

      fs.unlinkSync(concatedFileLocal);
      fs.unlinkSync(listFilePath);
    }

    return [
      concatedFileS3
    ];
  }

exports.handler = async (event, context) => {
  let recordId = event.record_id; 
  let durationTotal = 0;
  let sizeTotal = 0;
  let thumbnailFirst = '';
  let fileLists = [];
  let files = event.files;
  let concatedFiles = await concatFiles(files);
  let pathThumbnailTmp = `${efsPath}/${globalPath.dirname(files[0])}/${getRandomString(60)}.jpg`;
  
  await Promise.all(concatedFiles.map(async (key, index) => {
      
      const srcKey = decodeURIComponent(key).replace(/\+/g, ' ')
      const target = s3.getSignedUrl('getObject', { Bucket: bucket, Key: key, Expires: 1000 })
      let fileType = srcKey.match(/\.\w+$/)
      
      if (!fileType) {
        throw new Error(`invalid file type found for key: ${srcKey}`)
      }
    
      fileType = fileType[0].slice(1)
      if (allowedTypes.indexOf(fileType) === -1) {
        throw new Error(`filetype: ${fileType} is not an allowed type`)
      }
      
      const ffprobe = spawnSync(ffprobePath, [
        '-v',
        'error',
        '-show_entries',
        'format=duration',
        '-of',
        'default=nw=1:nk=1',
        target
      ]);
      
      let size = await sizeOf(key);
      sizeTotal += size;
      console.log('SIZE', size)
      let duration = Math.ceil(ffprobe.stdout.toString());
      durationTotal += duration;
      await createImage((duration * 0.25), target, pathThumbnailTmp);
      let dstKey = srcKey.replace(/\.\w+$/, `.jpg`);
      await uploadToS3(dstKey, pathThumbnailTmp);
      if (index === 0) {
        thumbnailFirst = dstKey;
      }
      
      fileLists.push({
        filename: key,
        size: size,
        thumbnail: dstKey,
        duration: duration
      });
      
      console.log(`processed ${bucket}/${srcKey} successfully`)
    }));

  //Call webhook
  let data = {
    duration: durationTotal,
    thumbnail: thumbnailFirst,
    files: fileLists,
    record_id: recordId,
    key: key,
    size: sizeTotal
  };
  
  console.log(`DATA ${JSON.stringify(data)}`)
  await callWebhookSuccess(data)
    .then(result => console.log(`Status code: ${JSON.stringify(result)}`))
    .catch(err => console.error(`Error doing the request for the event: ${JSON.stringify(event)} => ${err}`));
  return data;
}

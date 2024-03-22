'use strict';

var glob = require('glob');
var sizeOf = require('image-size');
const { execSync } = require('child_process');

var AWS = require('aws-sdk');
var s3 = new AWS.S3(); //{apiVersion: '2006-03-01'}

var shell = require('shelljs');
var fs = require('fs');
var path = require('path');
var sprintf = require('sprintf-js').sprintf;

const appName = process.env.APP_NAME;

const dstBucket = process.env.DESTINATION_BUCKET;

const hostPg = process.env.HOST_PG;
const portPg = process.env.PORT_PG;
const userPg = process.env.USER_PG;
const passwordPg = process.env.PASSWORD_PG;
const databasePg = process.env.DATABASE_PG;

const pg = require('pg');
const pool = new pg.Pool({
    host: hostPg,
    port: portPg,
    user: userPg,
    password: passwordPg,
    database: databasePg,
    max: 1,
    idleTimeoutMillis: 300000, // close idle clients after 300 second
    connectionTimeoutMillis: 10000, // return an error after 10 second if connection could not be established
});

var redis = require("redis");
const hostRedis = process.env.HOST_REDIS;
const portRedis = process.env.PORT_REDIS;
const databaseRedis = process.env.DATABASE_REDIS;

var redisClient = redis.createClient({
    host: hostRedis,
    port: portRedis,
    db: databaseRedis
}); //maxclients=65000

var serialized = require('php-serialize');
const moment = require('moment-timezone');

async function findDocument(documentId, columns) {
    const client = await pool.connect();

    try {
        var res = await client.query(`SELECT ${columns.join(',')} FROM documents WHERE id=${documentId}`);
        return res.rows[0];
    } finally {
        // https://github.com/brianc/node-postgres/issues/1180#issuecomment-270589769
        client.release(true);
    }
}

async function updateDocument(data, documentId) {
    const client = await pool.connect();

    try {
        let momentNow = moment().tz('Asia/Tokyo').format();
        data.updated_at = momentNow;

        let strSetData = Object.keys(data).map(key => `${key}='${data[key]}'`).join(","); //column='value'
        var res = await client.query(`UPDATE documents SET ${strSetData} WHERE id=${documentId}`);
        return res.rowCount > 0;
    } finally {
        client.release(true);
    }
}

async function downloadPdf(eventBucket, eventKey, pdfPath) {
    var params = {
        Bucket: eventBucket,
        Key: eventKey
    };

    const file = await s3.getObject(params).promise();
    //Add code valid only pdf file
    fs.writeFileSync(pdfPath, file.Body);
}

async function convertPdfImages(pdfPath, imagesFolder, titleName) {
    //Need check exists file
    execSync('pdftoppm -q -jpeg -r 200 ' + pdfPath + ' ' + imagesFolder + '/' + titleName); //toString()
}

async function convertPdfFirstImage(pdfPath, previewImageFolder, titleName) {
    execSync('pdftoppm -q -jpeg -r 200 -f 1 -singlefile ' + pdfPath + ' ' + previewImageFolder + '/' + titleName);
}

async function createThumbnailFirstImage(previewImageFolder, titleName) {
    var imagePath = previewImageFolder + '/' + titleName + '.jpg';
    var savePath = previewImageFolder + '/200x200_' + titleName + '.jpg';
    execSync('gm convert ' + imagePath + ' -thumbnail 200x200 -background white -gravity center -extent 200x200 ' + savePath);
}

async function createThumbnailImages(imagesFolder, thumbnailsFolder) {
    fs.readdirSync(imagesFolder).forEach(file => {
        var filePath = imagesFolder + '/' + file;
        var savePath = thumbnailsFolder + '/120x170_' + file;
        execSync('gm convert ' + filePath + ' -thumbnail 120x170 -background white -gravity center -extent 120x170 ' + savePath);
    });
}

async function uploadFolderToS3(userFolderPath) {
    let files = glob.sync(userFolderPath + '/**/*');
    for (var file of files) {
        if (fs.lstatSync(file).isDirectory()) {
            continue;
        }
        await uploadFileToS3(file);
    }
}

async function uploadFileToS3(pathFile) {
    var fileContent = fs.readFileSync(pathFile);
    var dstKey = pathFile.substring(5); //sub /tmp/

    const params = {
        // ACL: "public-read", //hide
        Body: fileContent,
        Bucket: dstBucket,
        Key: dstKey,
    };
    await s3.putObject(params).promise();
}

async function getTotalPages(imagesFolder) {
    let res = execSync(`ls ${imagesFolder} | wc -l`).toString();
    return parseInt(res);
}

async function getDimensionsImages(imagesFolder) {
    var images = {};
    var page = 1;
    fs.readdirSync(imagesFolder).forEach(file => {
        var dimensions = sizeOf(imagesFolder + '/' + file);

        images[page] = {
            width: dimensions.width,
            height: dimensions.height
        };
        page++;
    });
    return images;
}

async function cacheDocumentPathFiles(documentId, imagesFolder, thumbnailsFolder) {
    var images = [];
    var thumbnails = [];
    var imagePath = imagesFolder.substring(5); //sub /tmp/
    var thumbnailPath = thumbnailsFolder.substring(5); //sub /tmp/

    fs.readdirSync(imagesFolder).forEach(file => {
        images.push(imagePath + '/' + file);
    });
    fs.readdirSync(thumbnailsFolder).forEach(file => {
        thumbnails.push(thumbnailPath + '/' + file);
    });

    await redisClient.set('laravel:' + appName + '-document-file-' + documentId, serialized.serialize(images));
    await redisClient.set('laravel:' + appName + '-document-thumbnail-' + documentId, serialized.serialize(thumbnails));

    // redisClient.quit();
}

async function deleteCacheDocumentPathFiles(documentId) {
    await redisClient.del('laravel:' + appName + '-document-file-' + documentId);
    await redisClient.del('laravel:' + appName + '-document-thumbnail-' + documentId);
}

async function emptyLocalDirectory(dir) {
    execSync('rm -R ' + dir);
}

async function emptyS3Directory(bucket, dir) {
    const listParams = {
        Bucket: bucket,
        Prefix: dir
    };

    const listedObjects = await s3.listObjectsV2(listParams).promise();

    if (listedObjects.Contents.length === 0) return;

    const deleteParams = {
        Bucket: bucket,
        Delete: { Objects: [] }
    };

    listedObjects.Contents.forEach(({ Key }) => {
        deleteParams.Delete.Objects.push({ Key });
    });

    await s3.deleteObjects(deleteParams).promise();

    if (listedObjects.IsTruncated) await emptyS3Directory(bucket, dir);
}

async function checkIsUpdateDocument(isUpdate) {
    return isUpdate != undefined && isUpdate;
}

module.exports.handler = async (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false; //Require when query RDS, Redis

    var eventBucket = event.bucket;
    var eventKey = event.key;

    var documentId = event.document_id;
    var documentSize = event.size;
    var documentExtension = event.extension;

    var folderName = event.folder_name;
    var fileName = event.file_name; //hashed filename
    var extName = path.extname(fileName); //.pdf
    var titleName = path.basename(fileName, extName); //sample

    var userFolderPath = '/tmp/' + event.path_upload;
    var pdfFolder = '/tmp/' + event.path_pdf;
    var imagesFolder = '/tmp/' + event.path_image;
    var previewImageFolder = '/tmp/' + event.path_preview_image;
    var thumbnailsFolder = '/tmp/' + event.path_thumbnail_image;

    var pdfPath = pdfFolder + '/' + fileName;

    shell.mkdir('-p', pdfFolder);
    shell.mkdir('-p', imagesFolder);
    shell.mkdir('-p', previewImageFolder);
    shell.mkdir('-p', thumbnailsFolder);

    var documentInfo = await findDocument(documentId, ['id', 'has_run_queue']);
    if (documentInfo == undefined || documentInfo.has_run_queue) {
        return; //Handle duplicate invoke
    } else {
        await updateDocument({
            has_run_queue: true
        }, documentId);
    }

    await downloadPdf(eventBucket, eventKey, pdfPath);

    await convertPdfImages(pdfPath, imagesFolder, titleName);
    await convertPdfFirstImage(pdfPath, previewImageFolder, titleName);
    await createThumbnailFirstImage(previewImageFolder, titleName);
    await createThumbnailImages(imagesFolder, thumbnailsFolder);

    await uploadFolderToS3(userFolderPath);

    var totalPages = await getTotalPages(imagesFolder);
    var extendSizes = await getDimensionsImages(imagesFolder);

    var dataUpdate = {
        total_pages: totalPages,
        extend: JSON.stringify(extendSizes),
        status: 2,
        size: documentSize,
        extension: documentExtension
    };

    var isUpdateDocument = await checkIsUpdateDocument(event.is_update);
    if (isUpdateDocument) {
        dataUpdate.url_store = folderName;
        dataUpdate.filename = fileName;

        // await emptyS3Directory(dstBucket, sprintf("documents/%s/%s", event.user_id, event.url_store));//Deleted in backend
        await deleteCacheDocumentPathFiles(documentId);
    }

    await updateDocument(dataUpdate, documentId);

    //Need deleteCacheDocumentPathFiles before cacheDocumentPathFiles
    await cacheDocumentPathFiles(documentId, imagesFolder, thumbnailsFolder); //quotas 256 KB payload async

    await emptyLocalDirectory('/tmp/documents');
    await emptyS3Directory(eventBucket, event.path_upload); //s3_tmp

    callback(null, 'OK');
}

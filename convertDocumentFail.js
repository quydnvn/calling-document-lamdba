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
    idleTimeoutMillis: 3000 // close idle clients after 3 second, need set like config in Lambda
    // connectionTimeoutMillis: 10000, // return an error after 10 second if connection could not be established
});

const moment = require('moment-timezone');

async function updateStatusDocument(event, context) {
    const documentId = event.requestPayload.document_id; //Not info talkscriptId

    let momentNow = moment().tz('Asia/Tokyo').format();

    // No need update total_pages=0,extend={}
    // Upload document+talkscript but document fail, talkscript should update fail
    // Document+talkscript but talkscript fail, need update document has_run_queue=true
    var query = `
        UPDATE document_talkscripts SET status = 3, error = 'Error convert PDF', updated_at = '${momentNow}' WHERE document_id = ${documentId};
        UPDATE documents SET has_run_queue = true, status = 3, error = 'Error convert PDF', updated_at = '${momentNow}' WHERE id = ${documentId};
    `;

    const client = await pool.connect();

    try {
        await client.query(query);
    } finally {
        client.release(true);
    }
}

exports.handler = async (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false; //Require when query RDS

    await updateStatusDocument(event, context);

    callback(null, 'OK');
};

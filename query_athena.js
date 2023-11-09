const { fromEnv } = require('@aws-sdk/credential-providers');
const { Athena } = require('@aws-sdk/client-athena');
const fs = require('fs');

const filename = process.argv && process.argv[2];
const query = process.argv && process.argv[3];
if (!filename) {
    console.error('No filename provided');
    process.exit(1);
}
if (!query) {
    console.error('No query provided');
    process.exit(1);
}

const client = new Athena({
    region: 'eu-central-1',
    credentials: fromEnv() // Using Access key defined in env variables - Permissions are managed by the IAM roles
});

function createQueryExecution() {
    return new Promise((resolve, reject) => {
        /**doing resultConfiguration, but we will not save query result there. */
        const params = {
            QueryString: query /* required */,
            ResultConfiguration: {
                /* required */ OutputLocation: `s3://edc-dev-324690151932-eu-central-1-glue/athena/` /* required */,
                EncryptionConfiguration: {
                    EncryptionOption: 'SSE_S3' /* required */
                }
            }
        };
        client.startQueryExecution(params, (err, data) => {
            if (err) reject(err);
            resolve(data);
        });
    });
}

function checkQueryCreateStatus(queryobj) {
    return new Promise((resolve, reject) => {
        const params = {
            QueryExecutionId: queryobj.QueryExecutionId /* required */
        };
        client.getQueryExecution(params, (err, data) => {
            if (err) console.log(err, err.stack); // an error occurred
            else {
                if (
                    data &&
                    data.QueryExecution &&
                    data.QueryExecution.Status &&
                    data.QueryExecution.Status.State /* Statuses: QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED */
                ) {
                    console.log('Athena Query status is ' + data.QueryExecution.Status.State);
                    resolve(data.QueryExecution.Status.State);
                } else {
                    console.log('Atehna Query status is Active');
                    if (err) reject(err);
                    resolve(data);
                }
            }
        });
    });
}

function getQueryResultByExecutionId(queryobj) {
    return new Promise((resolve, reject) => {
        const params = {
            QueryExecutionId: queryobj.QueryExecutionId
        };
        client.getQueryResults(params, (err, data) => {
            if (err) reject(err);
            resolve(data);
        });
    });
}

function stopQueryExecutionId(queryobj) {
    return new Promise((resolve, reject) => {
        const params = {
            QueryExecutionId: queryobj.QueryExecutionId
        };
        client.stopQueryExecution(params, (err, data) => {
            if (err) reject(err);
            resolve(data);
        });
    });
}

function formatDataRow(columns, row) {
    const rowValues = row.Data.map(d => Object.values(d)[0]);
    let formattedData = {};

    for (const [i, v] of rowValues.entries()) {
        formattedData[columns[i]] = v;
    }
    return formattedData;
}

async function main() {
    let queryobj = await createQueryExecution();

    let running = true;
    while (running) {
        try {
            let status = await checkQueryCreateStatus(queryobj);
            if (status === 'SUCCEEDED') running = false;
            if (status === 'FAILED') process.exit(1);
        } catch (err) {
            console.error(err);
            process.exit(1);
        }
    }

    let result = await getQueryResultByExecutionId(queryobj);
    let end = await stopQueryExecutionId(queryobj);

    if (result.ResultSet.Rows.length > 1) {
        const columns = result.ResultSet.Rows[0].Data.map(c => c.VarCharValue);
        formatted = result.ResultSet.Rows.slice(1).map(r => formatDataRow(columns, r));
        fs.writeFileSync(`/home/ubuntu/${filename}.json`, JSON.stringify(formatted));
    } else {
        fs.writeFileSync(`/home/ubuntu/${filename}.json`, '[]');
    }
    process.exit(0);
}
main();

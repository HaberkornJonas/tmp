const { fromEnv } = require('@aws-sdk/credential-providers');
const { TimestreamQuery, QueryCommand } = require('@aws-sdk/client-timestream-query');
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

try {
    const client = new TimestreamQuery({
        region: 'us-east-1',
        credentials: fromEnv() // Using Access key defined in env variables - Permissions are managed by the IAM roles
    });

    const params = {
        QueryString: query
    };
    const command = new QueryCommand(params);

    function formatDataRow(columns, row) {
        const rowValues = row.Data.map(d => Object.values(d)[0]);
        let formattedData = {};

        for (const [i, v] of rowValues.entries()) {
            formattedData[columns[i]] = v;
        }
        return formattedData;
    }

    client.send(command).then(
        data => {
            const columns = data.ColumnInfo.map(c => c.Name);
            formatted = data.Rows.map(r => formatDataRow(columns, r));
            fs.writeFileSync(`/home/ubuntu/${filename}.json`, JSON.stringify(formatted));
            process.exit(0);
        },
        e => {
            console.error(e);
            process.exit(1);
        }
    );
} catch (err) {
    console.error(err);
    process.exit(1);
}

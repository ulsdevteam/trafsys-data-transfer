require('dotenv').config();
const oracledb = require('oracledb');
const axios = require('axios');
const qs = require('qs');
const datetime = require('node-datetime');

function checkEnv() {
  let keys = [
    'ORACLE_USER',
    'ORACLE_PASSWORD',
    'ORACLE_CONNECTION_STRING',
    'TRAFSYS_USER',
    'TRAFSYS_PASSWORD'
  ];
  let missingKeys = keys.filter(key => !(key in process.env));
  if (missingKeys.length === 0) return;
  console.error('Missing required environment variables: ' + missingKeys.join(', '));
  process.exit();
}

async function ensureTableExists(connection) {
  let result = await connection.execute(
    `select table_name
     from user_tables
     where table_name = 'ULS_TRAFSYS_DATA'`
  );
  if (result.rows.length === 0) {
    await connection.execute(
      `create table ULS_TRAFSYS_DATA
       (
         RecordId varchar2(100),
         SiteCode varchar2(100),
         Location varchar2(100),
         IsInternal number(1),
         PeriodEnding date,
         Ins number,
         Outs number,
         primary key(RecordId)
       )`
    );
  }
}

async function insertData(connection, data) {
  let result = await connection.executeMany(
    `insert into ULS_TRAFSYS_DATA values
     (
       :RecordId,
       :SiteCode,
       :Location,
       :IsInternal,
       TO_DATE(:PeriodEnding, 'YYYY-MM-DD"T"HH24:MI:SS'),
       :Ins,
       :Outs
     )`,
    data,
    {
      autoCommit: true,
      bindDefs: {
        RecordId: { type: oracledb.STRING, maxSize: 100 },
        SiteCode: { type: oracledb.STRING, maxSize: 100 },
        Location: { type: oracledb.STRING, maxSize: 100 },
        IsInternal: { type: oracledb.NUMBER },
        PeriodEnding: { type: oracledb.STRING, maxSize: 100 },
        Ins: { type: oracledb.NUMBER },
        Outs: { type: oracledb.NUMBER }
      }
    }
  );
  console.log('Inserted ' + result.rowsAffected + ' records.');
}

async function run() {
  let connection;
  try {
    console.log('Connecting to database... ');
    connection = await oracledb.getConnection({
      user: process.env.ORACLE_USER,
      password: process.env.ORACLE_PASSWORD,
      connectString: process.env.ORACLE_CONNECTION_STRING
    });
    // check if table exists, if not create it
    await ensureTableExists(connection);
    // get data from trafsys api
    let trafsysData = await getTrafsysData();
    // insert data into table
    await insertData(connection, trafsysData);  
  }
  catch (e) {
    console.error(e);
  }
  finally {
    if (connection) {
      await connection.close();
    }
  }
}

function yesterdayDateString() {
  var date = datetime.create();
  date.offsetInDays(-1);
  return date.format('Y-m-d');
}

async function getTrafsysData() {
  const trafsysUrl = 'https://portal.trafnet.com/rest/';
  let tokenResponse = await axios.post(trafsysUrl + 'token', qs.stringify({
    username: process.env.TRAFSYS_USER,
    password: process.env.TRAFSYS_PASSWORD,
    grant_type: 'password'
  }), {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  });
  let access_token = tokenResponse.data.access_token; 
  let yesterday = yesterdayDateString();
  let dataResponse = await axios.get(trafsysUrl + 'api/traffic', {
    params: {
      SiteCode: '',
      IncludeInternalLocations: true,
      DataSummedByDay: false,
      DateFrom: yesterday,
      DateTo: yesterday
    },
    headers: {
      'Authorization': 'Bearer ' + access_token
    }
  });
  let data = dataResponse.data;
  for (let record of data) {
    record.RecordId = generatePrimaryKey(record);
    // Oracle has no boolean datatype for columns, so cast it to a number
    record.IsInternal = +record.IsInternal;
  }
  return data;
}

function generatePrimaryKey(record) {
  return (
    record.SiteCode + 
    record.Location +
    record.PeriodEnding.split(':')[0]
  ).replace(/[^a-z0-9]/gi, '');
}

checkEnv();
run();

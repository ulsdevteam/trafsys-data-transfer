require('dotenv').config();
const oracledb = require('oracledb');
const axios = require('axios');
const qs = require('qs');
const datetime = require('node-datetime');
const { program } = require('commander');

const yesterday = yesterdayDateString();
program
  .option('-f, --from <date>', 'From Date (YYYY-MM-DD)', yesterday)
  .option('-t, --to <date>', 'To Date (YYYY-MM-DD)', yesterday);
program.parse();

/**
 * Checks if all the required environment variables are present.
 * If they are not, display an error and quit.
 */
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

/**
 * Creates the ULS_TRAFSYS_DATA table if it does not exist.
 * @param {oracledb.Connection} connection - The database connection.
 */
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

/**
 * A TrafSys data record.
 * @typedef {Object} DataRecord
 * @property {string} RecordId - Uniquely identifying ID composed from SiteCode, Location, and PeriodEnding.
 * @property {string} SiteCode - The alphanumeric code that identifies the site within the organization.
 * @property {string} Location - The name of the location where the sensors are counting.
 * @property {number} IsInternal - Indicates (using 0 or 1) whether this is an internal location.
 * @property {string} PeriodEnding - The end of the hour-long time period this record corresponds to.
 * @property {number} Ins - The in counts for that time period and location.
 * @property {number} Outs - The out counts for that time period and location.
 */

/**
 * Retrieves TrafSys data from the REST API.
 * @returns {Promise<DataRecord[]>} Data pulled from the api and given a RecordId.
 */
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
  let options = program.opts(); 
  let dataResponse = await axios.get(trafsysUrl + 'api/traffic', {
    params: {
      SiteCode: '',
      IncludeInternalLocations: true,
      DataSummedByDay: false,
      DateFrom: options.from,
      DateTo: options.to
    },
    headers: {
      'Authorization': 'Bearer ' + access_token
    }
  });
  let data = dataResponse.data;
  for (let record of data) {
    setPrimaryKey(record);
    // Oracle has no boolean datatype for columns, so cast it to a number
    record.IsInternal = +record.IsInternal;
  }
  return data;
}

/**
 * @returns {string} Yesterday's date formatted as a YYYY-MM-DD string.
 */
function yesterdayDateString() {
  var date = datetime.create();
  date.offsetInDays(-1);
  return date.format('Y-m-d');
}

/**
 * Generates and sets the primary key for a record.
 * @param {Partial<DataRecord>} record
 */
function setPrimaryKey(record) {
  record.RecordId = (
    record.SiteCode + 
    record.Location +
    record.PeriodEnding.split(':')[0]
  ).replace(/[^a-z0-9]/gi, '');
}

/**
 * Inserts the TrafSys data into the database.
 * @param {oracledb.Connection} connection - The database connection.
 * @param {DataRecord[]} data 
 */
async function insertData(connection, data) {
  let result = await connection.executeMany(
    `begin
       insert into ULS_TRAFSYS_DATA values
       (
         :RecordId,
         :SiteCode,
         :Location,
         :IsInternal,
         TO_DATE(:PeriodEnding, 'YYYY-MM-DD"T"HH24:MI:SS'),
         :Ins,
         :Outs
       );
     exception when dup_val_on_index then
       update ULS_TRAFSYS_DATA
       set Ins = :Ins, Outs = :Outs
       where RecordId = :RecordId;
     end;`,
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
}

/**
 * Run the program, pulling data from TrafSys and inserting it into the database.
 */
async function run() {
  checkEnv();
  let connection;
  try {
    connection = await oracledb.getConnection({
      user: process.env.ORACLE_USER,
      password: process.env.ORACLE_PASSWORD,
      connectString: process.env.ORACLE_CONNECTION_STRING
    });
    await ensureTableExists(connection);
    let trafsysData = await getTrafsysData();
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

run();

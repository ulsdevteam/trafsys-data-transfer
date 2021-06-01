const path = require('path');
require('dotenv').config({ path: path.dirname(process.argv[1]) + '/.env' });
const oracledb = require('oracledb');
const axios = require('axios');
const qs = require('qs');
const datetime = require('node-datetime');
const { program } = require('commander');
const Datastore = require('nedb');

const logsDb = new Datastore({ filename: path.dirname(process.argv[1]) + '/logs.db', autoload: true, timestampData: true });
logsDb.ensureIndex({ fieldName: "createdAt" });
const yesterday = yesterdayDateString();
const trafsysUrl = 'https://portal.trafnet.com/rest/';

/**
 * A log object that collects info related to a run of this program.
 * @typedef {Object} RunInfo
 * @property {string} AccessToken - The access token used to get Trafsys data. Will be reused until it expires.
 * @property {Date} AccessTokenExpiresAt - The time at which the access token expires.
 * @property {string} FromDate - The From date used for this run, in YYYY-MM-DD format.
 * @property {string} ToDate - The To date used for this run, in YYYY-MM-DD format.
 * @property {number?} Records - The number of records written to the Oracle db.
 */

/**
 * Generates a RunInfo object for the current run.
 * @returns {Promise<RunInfo>}
 */
async function getRunInfo() {
  // helper function to call nedb cursor methods using async/await syntax
  let execAsync = cursor => new Promise(
    (resolve, reject) => cursor.exec((err, result) => err ? reject(err) : resolve(result))
  );
  // use sort and limit to get most recently saved log object
  let previousRun = await execAsync(logsDb.findOne({}).sort({ createdAt: -1 }).limit(1));
  let currentRun = {};
  if (previousRun) {
    let expiresAt = datetime.create(previousRun.AccessTokenExpiresAt);
    let nowish = datetime.create();
    // Offset by 5 minutes to give some wiggle room (technical term)
    nowish.offsetInHours(-1/12);
    // .getTime() converts the object to a timestamp for comparison
    if (expiresAt.getTime() > nowish.getTime()) {
      currentRun.AccessToken = previousRun.AccessToken;
      currentRun.AccessTokenExpiresAt = previousRun.AccessTokenExpiresAt;
    }
  }
  if (!currentRun.AccessToken) {
    let tokenData = await getAccessToken();
    currentRun.AccessToken = tokenData.access_token;
    currentRun.AccessTokenExpiresAt = new Date(tokenData[".expires"]);
  }
  program
    .option('-f, --from <date>', 'From Date (YYYY-MM-DD)', previousRun?.ToDate || yesterday)
    .option('-t, --to <date>', 'To Date (YYYY-MM-DD)', yesterday);
  program.parse();
  let opts = program.opts();
  currentRun.FromDate = opts.from;
  currentRun.ToDate = opts.to;
  return currentRun;
}

/**
 * Gets a fresh access token from TrafSys.
 * 
 * @returns {Promise<{access_token: string, ".expires": string}>}
 */
async function getAccessToken() {
  let tokenResponse = await axios.post(trafsysUrl + 'token', qs.stringify({
    username: process.env.TRAFSYS_USER,
    password: process.env.TRAFSYS_PASSWORD,
    grant_type: 'password'
  }), {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  });
  return tokenResponse.data;
}

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
         SiteCode varchar2(100),
         Location varchar2(100),
         IsInternal number(1),
         PeriodEnding date,
         Ins number,
         Outs number,
         primary key(SiteCode, Location, PeriodEnding)
       )`
    );
  }
}

/**
 * A TrafSys data record.
 * @typedef {Object} DataRecord
 * @property {string} SiteCode - The alphanumeric code that identifies the site within the organization.
 * @property {string} Location - The name of the location where the sensors are counting.
 * @property {number} IsInternal - Indicates (using 0 or 1) whether this is an internal location.
 * @property {string} PeriodEnding - The end of the hour-long time period this record corresponds to.
 * @property {number} Ins - The in counts for that time period and location.
 * @property {number} Outs - The out counts for that time period and location.
 */

/**
 * Retrieves TrafSys data from the REST API.
 * @param {RunInfo} runInfo - The run information for the current run.
 * @returns {Promise<DataRecord[]>} Data pulled from the api and given a RecordId.
 */
async function getTrafsysData(runInfo) { 
  let dataResponse = await axios.get(trafsysUrl + 'api/traffic', {
    params: {
      SiteCode: '',
      IncludeInternalLocations: true,
      DataSummedByDay: false,
      DateFrom: runInfo.FromDate,
      DateTo: runInfo.ToDate,
    },
    headers: {
      'Authorization': 'Bearer ' + runInfo.AccessToken
    }
  });
  let data = dataResponse.data;
  for (let record of data) {
    // Oracle has no boolean datatype for columns, so cast it to a number
    record.IsInternal = +record.IsInternal;
  }
  runInfo.Records = data.length;
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
 * Inserts the TrafSys data into the database.
 * @param {oracledb.Connection} connection - The database connection.
 * @param {DataRecord[]} data  
 */
async function insertData(connection, data) {
  if (data.length === 0) return;
  let result = await connection.executeMany(
    `begin
       insert into ULS_TRAFSYS_DATA values
       (
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
       where SiteCode = :SiteCode
         and Location = :Location
         and PeriodEnding = TO_DATE(:PeriodEnding, 'YYYY-MM-DD"T"HH24:MI:SS');
     end;`,
    data,
    {
      autoCommit: true,
      bindDefs: {
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
 * Returns a promise that resolves after one second.
 * @returns {Promise<void>}
 */
function waitASecond() {
  return new Promise(resolve => setTimeout(resolve, 1000));
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
    let runInfo = await getRunInfo();
    let trafsysData;
    try {
      trafsysData = await getTrafsysData(runInfo);
    }
    catch (e) {
      if (e.isAxiosError && e.response.status == 401) {
        // wait a second to prevent "429 Too Many Requests"
        await waitASecond();
        let tokenData = await getAccessToken();
        runInfo.AccessToken = tokenData.access_token;
        runInfo.AccessTokenExpiresAt = new Date(tokenData[".expires"]);
        trafsysData = await getTrafsysData(runInfo);
      } else {
        throw e;
      }      
    }
    await insertData(connection, trafsysData);
    logsDb.insert(runInfo);
  }
  catch (e) {
    console.error(e.toString());
  }
  finally {
    if (connection) {
      await connection.close();
    }
  }
}

run();

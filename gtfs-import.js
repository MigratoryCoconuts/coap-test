const gtfs = require('gtfs');
const config = require('./gtfs-import-config.json');

gtfs.import(config, (err) => {
  if (err) return console.error(err);

  console.log('Import Successful')
});
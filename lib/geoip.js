const fsp = require('fs').promises
var net = require('net');
var path = require('path');

var utils = require('./utils');
var fsWatcher = require('./fsWatcher');

var watcherName = 'dataWatcher';


var privateRange4 = [
  [utils.aton4('10.0.0.0'), utils.aton4('10.255.255.255')],
  [utils.aton4('172.16.0.0'), utils.aton4('172.31.255.255')],
  [utils.aton4('192.168.0.0'), utils.aton4('192.168.255.255')],
  [utils.aton4('192.168.0.0'), utils.aton4('192.168.255.255')],
  [utils.aton4('169.254.0.0'), utils.aton4('169.254.255.255')],
  [utils.aton4('127.0.0.0'), utils.aton4('127.255.255.255')],
];

var privateRange6 = [
  [utils.aton6('fd00::'), utils.aton6('fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')],
  [utils.aton6('fe80::'), utils.aton6('febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff')],
];

var conf4 = {
  firstIP: null,
  lastIP: null,
  lastLine: 0,
  locationBuffer: null,
  locationRecordSize: 88,
  mainBuffer: null,
  recordSize: 24
};

var conf6 = {
  firstIP: null,
  lastIP: null,
  lastLine: 0,
  mainBuffer: null,
  recordSize: 48
};

var RECORD_SIZE = 10;
var RECORD_SIZE6 = 34;

class GeoIP {
  constructor(dataPath) {
    this.init(dataPath)

    //copy original configs
    this.cache4 = JSON.parse(JSON.stringify(conf4));
    this.cache6 = JSON.parse(JSON.stringify(conf6));

    let initResolve, initReject
    this.ready = new Promise((resolve,reject) => {
      initResolve = resolve
      initReject = reject
    })

    // initial data loading is async now
    // wait a couple seconds for it to be ready
    this.reloadData().then(()=>{
      initResolve()
    }).catch(e => {
      initReject(e)
    })
  }

  init(dataPath) {
    if (dataPath) {
      this.geodatadir = dataPath
    } else if (typeof global.geodatadir === 'undefined') {
      this.geodatadir = path.join(__dirname, '/../data/');
    } else {
      this.geodatadir = global.geodatadir;
    }

    this.dataFiles = {
      city: path.join(this.geodatadir, 'geoip-city.dat'),
      city6: path.join(this.geodatadir, 'geoip-city6.dat'),
      cityNames: path.join(this.geodatadir, 'geoip-city-names.dat'),
      country: path.join(this.geodatadir, 'geoip-country.dat'),
      country6: path.join(this.geodatadir, 'geoip-country6.dat')
    };
  }

  async waitForReady() {
    return this.ready
  }

  lookup4(ip) {
    var fline = 0;
    var floor = this.cache4.lastIP;
    var cline = this.cache4.lastLine;
    var ceil = this.cache4.firstIP;
    var line;
    var locId;

    var buffer = this.cache4.mainBuffer;
    var locBuffer = this.cache4.locationBuffer;
    var recordSize = this.cache4.recordSize;
    var locRecordSize = this.cache4.locationRecordSize;

    var i;

    var geodata = {
      range: '',
      country: '',
      region: '',
      eu: '',
      timezone: '',
      city: '',
      ll: [0, 0]
    };

    // outside IPv4 range
    if (ip > this.cache4.lastIP || ip < this.cache4.firstIP) {
      return null;
    }

    // private IP
    for (i = 0; i < privateRange4.length; i++) {
      if (ip >= privateRange4[i][0] && ip <= privateRange4[i][1]) {
        return null;
      }
    }

    do {
      line = Math.round((cline - fline) / 2) + fline;
      floor = buffer.readUInt32BE(line * recordSize);
      ceil = buffer.readUInt32BE((line * recordSize) + 4);

      if (floor <= ip && ceil >= ip) {
        geodata.range = [floor, ceil];

        if (recordSize === RECORD_SIZE) {
          geodata.country = buffer.toString('utf8', (line * recordSize) + 8, (line * recordSize) + 10);
        } else {
          locId = buffer.readUInt32BE((line * recordSize) + 8);

          // -1>>>0 is a marker for "No Location Info"
          if(-1>>>0 > locId) {
            geodata.country = locBuffer.toString('utf8', (locId * locRecordSize) + 0, (locId * locRecordSize) + 2).replace(/\u0000.*/, '');
            geodata.region = locBuffer.toString('utf8', (locId * locRecordSize) + 2, (locId * locRecordSize) + 5).replace(/\u0000.*/, '');
            geodata.metro = locBuffer.readInt32BE((locId * locRecordSize) + 5);
            geodata.ll[0] = buffer.readInt32BE((line * recordSize) + 12) / 10000;//latitude
            geodata.ll[1] = buffer.readInt32BE((line * recordSize) + 16) / 10000; //longitude
            geodata.area = buffer.readUInt32BE((line * recordSize) + 20); //longitude
            geodata.eu = locBuffer.toString('utf8', (locId * locRecordSize) + 9, (locId * locRecordSize) + 10).replace(/\u0000.*/, '');
            geodata.timezone = locBuffer.toString('utf8', (locId * locRecordSize) + 10, (locId * locRecordSize) + 42).replace(/\u0000.*/, '');
            geodata.city = locBuffer.toString('utf8', (locId * locRecordSize) + 42, (locId * locRecordSize) + locRecordSize).replace(/\u0000.*/, '');
          }
        }

        return geodata;
      } else if (fline === cline) {
        return null;
      } else if (fline === (cline - 1)) {
        if (line === fline) {
          fline = cline;
        } else {
          cline = fline;
        }
      } else if (floor > ip) {
        cline = line;
      } else if (ceil < ip) {
        fline = line;
      }
    } while (1);
  }

  lookup6(ip) {
    var buffer = this.cache6.mainBuffer;
    var recordSize = this.cache6.recordSize;
    var locBuffer = this.cache4.locationBuffer;
    var locRecordSize = this.cache4.locationRecordSize;

    var geodata = {
      range: '',
      country: '',
      region: '',
      city: '',
      ll: [0, 0]
    };
    function readip(line, offset) {
      var ii = 0;
      var ip = [];

      for (ii = 0; ii < 2; ii++) {
        ip.push(buffer.readUInt32BE((line * recordSize) + (offset * 16) + (ii * 4)));
      }

      return ip;
    }

    this.cache6.lastIP = readip(this.cache6.lastLine, 1);
    this.cache6.firstIP = readip(0, 0);

    var fline = 0;
    var floor = this.cache6.lastIP;
    var cline = this.cache6.lastLine;
    var ceil = this.cache6.firstIP;
    var line;
    var locId;

    if (utils.cmp6(ip, this.cache6.lastIP) > 0 || utils.cmp6(ip, this.cache6.firstIP) < 0) {
      return null;
    }

    // private IP
    for (let i = 0; i < privateRange6.length; i++) {
      if (utils.cmp6(ip, privateRange6[i][0]) >= 0 && utils.cmp6(ip, privateRange6[i][1]) <= 0) {
        return null;
      }
    }

    do {
      line = Math.round((cline - fline) / 2) + fline;
      floor = readip(line, 0);
      ceil = readip(line, 1);

      if (utils.cmp6(floor, ip) <= 0 && utils.cmp6(ceil, ip) >= 0) {
        if (recordSize === RECORD_SIZE6) {
          geodata.country = buffer.toString('utf8', (line * recordSize) + 32, (line * recordSize) + 34).replace(/\u0000.*/, '');
        } else {
          locId = buffer.readUInt32BE((line * recordSize) + 32);

          geodata.country = locBuffer.toString('utf8', (locId * locRecordSize) + 0, (locId * locRecordSize) + 2).replace(/\u0000.*/, '');
          geodata.region = locBuffer.toString('utf8', (locId * locRecordSize) + 2, (locId * locRecordSize) + 5).replace(/\u0000.*/, '');
          geodata.metro = locBuffer.readInt32BE((locId * locRecordSize) + 5);
          geodata.ll[0] = buffer.readInt32BE((line * recordSize) + 36) / 10000;//latitude
          geodata.ll[1] = buffer.readInt32BE((line * recordSize) + 40) / 10000; //longitude
          geodata.area = buffer.readUInt32BE((line * recordSize) + 44); //area
          geodata.eu = locBuffer.toString('utf8', (locId * locRecordSize) + 9, (locId * locRecordSize) + 10).replace(/\u0000.*/, '');
          geodata.timezone = locBuffer.toString('utf8', (locId * locRecordSize) + 10, (locId * locRecordSize) + 42).replace(/\u0000.*/, '');
          geodata.city = locBuffer.toString('utf8', (locId * locRecordSize) + 42, (locId * locRecordSize) + locRecordSize).replace(/\u0000.*/, '');
        }
        // We do not currently have detailed region/city info for IPv6, but finally have coords
        return geodata;
      } else if (fline === cline) {
        return null;
      } else if (fline === (cline - 1)) {
        if (line === fline) {
          fline = cline;
        } else {
          cline = fline;
        }
      } else if (utils.cmp6(floor, ip) > 0) {
        cline = line;
      } else if (utils.cmp6(ceil, ip) < 0) {
        fline = line;
      }
    } while (1);
  }

  get4mapped(ip) {
    var ipv6 = ip.toUpperCase();
    var v6prefixes = ['0:0:0:0:0:FFFF:', '::FFFF:'];
    for (var i = 0; i < v6prefixes.length; i++) {
      var v6prefix = v6prefixes[i];
      if (ipv6.indexOf(v6prefix) == 0) {
        return ipv6.substring(v6prefix.length);
      }
    }
    return null;
  }

  async preload() {
    var datFile;
    var datSize;
    try {
      datFile = await fsp.open(this.dataFiles.cityNames, 'r');
      datSize = (await datFile.stat()).size;

      if (datSize === 0) {
        throw {
          code: 'EMPTY_FILE'
        };
      }

      this.cache4.locationBuffer = Buffer.alloc(datSize);
      await datFile.read(this.cache4.locationBuffer, 0, datSize, 0);
      await datFile.close();

      datFile = await fsp.open(this.dataFiles.city, 'r');
      datSize = (await datFile.stat()).size;
    } catch (err) {
      if (err.code !== 'ENOENT' && err.code !== 'EBADF' && err.code !== 'EMPTY_FILE') {
        throw err;
      }

      datFile = await fsp.open(this.dataFiles.country, 'r');
      datSize = (await datFile.stat()).size;
      this.cache4.recordSize = RECORD_SIZE;
    }

    this.cache4.mainBuffer = Buffer.alloc(datSize);
    await datFile.read(this.cache4.mainBuffer, 0, datSize, 0);

    await datFile.close();

    this.cache4.lastLine = Math.trunc(datSize / this.cache4.recordSize) - 1;
    this.cache4.lastIP = this.cache4.mainBuffer.readUInt32BE((this.cache4.lastLine * this.cache4.recordSize) + 4);
    this.cache4.firstIP = this.cache4.mainBuffer.readUInt32BE(0);
  }

  async preload6() {
    var datFile;
    var datSize;
    try {
      datFile = await fsp.open(this.dataFiles.city6, 'r');
      datSize = (await datFile.stat()).size;

      if (datSize === 0) {
        throw {
          code: 'EMPTY_FILE'
        };
      }
    } catch (err) {
      if (err.code !== 'ENOENT' && err.code !== 'EBADF' && err.code !== 'EMPTY_FILE') {
        throw err;
      }

      datFile = await fsp.open(this.dataFiles.country6, 'r');
      datSize = (await datFile.stat()).size;
      this.cache6.recordSize = RECORD_SIZE6;
    }

    this.cache6.mainBuffer = Buffer.alloc(datSize);
    await datFile.read(this.cache6.mainBuffer, 0, datSize, 0);

    await datFile.close();

    this.cache6.lastLine = Math.trunc(datSize / this.cache6.recordSize) - 1;
  }

  cmp = utils.cmp

  lookup(ip) {
    if (!ip) {
      return null;
    } else if (typeof ip === 'number') {
      return this.lookup4(ip);
    } else if (net.isIP(ip) === 4) {
      return this.lookup4(utils.aton4(ip));
    } else if (net.isIP(ip) === 6) {
      var ipv4 = this.get4mapped(ip);
      if (ipv4) {
        return this.lookup4(utils.aton4(ipv4));
      } else {
        return this.lookup6(utils.aton6(ip));
      }
    }

    return null;
  }

  pretty(n) {
    if (typeof n === 'string') {
      return n;
    } else if (typeof n === 'number') {
      return utils.ntoa4(n);
    } else if (n instanceof Array) {
      return utils.ntoa6(n);
    }

    return n;
  }

  // Start watching for data updates. The watcher waits one minute for file transfer to
  // completete before triggering the callback.
  startWatchingDataUpdate(callback) {
    fsWatcher.makeFsWatchFilter(watcherName, this.geodatadir, 60 * 1000, async function () {
      await this.preload()
      await this.preload6()
      callback()
    });
  }

  // Stop watching for data updates.
  stopWatchingDataUpdate() {
    fsWatcher.stopWatching(watcherName);
  }

  //clear data
  clear() {
    this.cache4 = JSON.parse(JSON.stringify(conf4));
    this.cache6 = JSON.parse(JSON.stringify(conf6));
  }

  // Reload data asynchronously
  async reloadData() {
    await this.preload().catch(err => console.log('Error loading v4 DB', err))
    await this.preload6().catch(err => console.log('Error loading v6 DB', err))
  }
}

module.exports = GeoIP

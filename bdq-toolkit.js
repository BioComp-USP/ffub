var fs = require('fs');
var CsvReader = require('csv-reader');
var hash = require('object-hash');
var request = require('request');
var key = process.env.GMAPS_KEY;
if(!key)
    console.log("This software uses Google Maps API (Geocoding). Please, generate a proper key on Google Cloud console e set the environment variable GMAPS_KEY with the key.");
var googleMapsClient = require('@google/maps').createClient({key: key});
var Toolkit = function(ffub){
    this.ffub = ffub;
}
// Read dataset from a DwC file
Toolkit.prototype.readDatasetFromDwCFile = function(path, limit=Infinity, delimiter="\t", callbackRecord){
    var self = this;
    var header = [];    
    var inputStream = fs.createReadStream(path, 'utf8');  
    var data = [];      
    inputStream.pipe(CsvReader({ parseNumbers: true, delimiter:delimiter, parseBooleans: true, trim: true }))
        .on('data', function (row) {
            if(header.length==0)
                header = row;
            else {                
                if(data.length<limit){
                    var r = {};
                    for(var h = 0; h<header.length; h++){                                
                        r[header[h]] = row[h];                
                    }                    
                    callbackRecord(r);
                    data.push(r);                
                } 
            }                
        })
        .on('end', function (data_) {
            // if(callbackDataset) callbackDataset(data);                         
        });
}
Toolkit.prototype.measureCoordinatesCompletenessRecord = function(record) {
    var rs = "not complete";
    if(record.decimalLatitude && String(record.decimalLatitude).trim().length>0 
            && record.decimalLongitude && String(record.decimalLongitude).trim().length>0 
            && !(Number(record.decimalLongitude) == 0 && Number(record.decimalLongitude)==0))
        rs = "complete";
    return rs;
}
Toolkit.prototype.coordinatesConsistencyGMaps = function(record, cb){    
    if(record.decimalLatitude && record.decimalLongitude && (record.country || record.countryCode)){
        googleMapsClient.geocode({
        address: `${record.decimalLatitude}, ${record.decimalLongitude}`
        }, function(err, response) {            
            if (!err) {
                if(response.json.results[0]){
                    var consistent = false;
                    response.json.results[0].address_components.forEach(function(item){
                        if(item.types.indexOf("country")>=0){
                            if((record.country && item.long_name.trim().toUpperCase() == record.country.trim().toUpperCase()) 
                                || (record.countryCode && item.short_name.trim().toUpperCase() == record.countryCode.trim().toUpperCase())){
                                consistent = true;
                                return false;
                            }                            
                        }
                    })
                    cb(consistent?"consistent":"not consistent")
                } else {
                    cb("unknown");
                }                                    
            } else {                
                cb("unknown");
            }
        });
    }  else cb("unknown")  
}
Toolkit.prototype.recommendCoordinatesSingleFromMunicipality = function(record,cb){    
    var q = record.municipality || record.locality;
    googleMapsClient.geocode({
    address: q
    }, function(err, response) {            
        if (!err) {
            if(response.json.results[0]){                
                if(response.json.results[0].geometry && response.json.results[0].geometry.location && response.json.results[0].geometry.location.lat && response.json.results[0].geometry.location.lng)
                    cb({decimalLatitude: response.json.results[0].geometry.location.lat, decimalLongitude: response.json.results[0].geometry.location.lng});
                else 
                    cb("unknown");
            } else {
                cb("unknown");
            }                                    
        } else {                
            cb("unknown");
        }
    });    
}

Toolkit.prototype.measureCoordinatesCompletenessDataset = function(dataset,dimensionId){  
    var self = this;
    var total = dataset.length;
    if(!total) 
        return 0;
    var result = 0;
    dataset.forEach(function(rec){
        self.ffub.retrieveMeasuresByDataResourceId(rec.id,dimensionId).forEach(function(measure){
            if(measure.result=="complete")
                result = result + (1/total);
        });
    });    
    return result;
}
Toolkit.prototype.measureCoordinatesConsistencyDataset = function(dataset,dimensionId){  
    var self = this;
    var total = dataset.length;
    if(!total) 
        return 0;
    var result = 0;
    dataset.forEach(function(rec){
        self.ffub.retrieveMeasuresByDataResourceId(rec.id,dimensionId).forEach(function(measure){
            if(measure.result=="consistent")
                result = result + (1/total);
        });
    });    
    return result;
}
module.exports = Toolkit;
var fs = require('fs');
var CsvReader = require('csv-reader');
var hash = require('object-hash');

var Toolkit = function(ffub){
    this.ffub = ffub;
}
// Read dataset from a DwC file
Toolkit.prototype.readDatasetFromDwCFile = function(path, limit=Infinity, delimiter="\t", callbackRecord, callbackDataset){
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
            if(callbackDataset) callbackDataset(data);                         
        });
}

Toolkit.prototype.measureCoordinatesCompletenessSingle = function(record){    
    if(
        typeof record.decimalLatitude != "undefinded" &&
        typeof record.decimalLongitude != "undefinded" &&
        record.decimalLatitude != null &&
        record.decimalLongitude != null &&
        String(record.decimalLatitude).length>0 &&
        String(record.decimalLongitude).length>0
        )
        return "complete";
    return "not complete";
}
Toolkit.prototype.validateCoordinatesCompletenessSingleFromMeasureId = function(measureId){    
    return (this.ffub.db.assertion.measure[measureId].result=="complete");
}
Toolkit.prototype.recommendCoordinatesSingleFromVerbatimLocality = function(record){    
    return String(record.locality).length>0 && String(record.locality).trim().toUpperCase()!="NÃO INFORMADO" ?{decimalLatitude:-30, decimalLongitude:-63}:null;    
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
module.exports = Toolkit;
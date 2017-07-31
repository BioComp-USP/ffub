// Load NodeJS modules
var Toolkit = require("./bdq-toolkit");
var FFUB = require("./index");
var async = require("async");

// Instance of of FFUB
var ffub = new FFUB();

// Instance of mechanism that will generates assertions results
var mechanism = new Toolkit(ffub);

// Params to read the Data Resource file 
var filePath = 'occurrence.txt', limit = 20, delimiter = "\t";

// 1. REGISTRY - DQ Needs

// Registring Use Case
var useCaseId = ffub.registryUseCase({name: "A simple example", author:"Allan Koch Veiga"});

// Registring Information Element
var ieCoordinatesId = ffub.registryIE({name: "Coordinates", dwc:["decimalLatitude", "decimalLongitude", "geodeticDatum"], author:"Allan Koch Veiga"});
var ieSpeciesNameId = ffub.registryIE({name: "Species name", dwc:["scientificName"], author:"Allan Koch Veiga"});
var ieCountryCodeId = ffub.registryIE({name: "Country", dwc:["country"], author:"Allan Koch Veiga"});
var ieMunicipalityId = ffub.registryIE({name: "Municipality", dwc:["municipality"], author:"Allan Koch Veiga"});
var ieLocalityId = ffub.registryIE({name: "Locality", dwc:["locality"], author:"Allan Koch Veiga"});

// Registring Dimension
var dimensionAId = ffub.registryDimension({name: "Coordinates completeness", resourceType:"single", ieId: ieCoordinatesId, dimension:"completeness", description:"Latitude and longitude supplied are complete", author:"Allan Koch Veiga"});
var dimensionBId = ffub.registryDimension({name: "Coordinates completeness", resourceType:"multi", ieId: ieCoordinatesId,  dimension:"completeness", description:"Proportion of latitude and longitude supplied", author:"Allan Koch Veiga"});
var dimensionCId = ffub.registryDimension({name: "Coordinates consistency", resourceType:"single", ieId: ieCoordinatesId, dimension:"consistency", description:"Latitude and longitude supplied are consistent with country name", author:"Allan Koch Veiga"});
var dimensionDId = ffub.registryDimension({name: "Coordinates consistency", resourceType:"multi", ieId: ieCoordinatesId, dimension:"consistency", description:"Proportion of latitude and longitude consistent with country name", author:"Allan Koch Veiga"});

// Registring Criterion
var criterionId = ffub.registryCriterion({statement: "Coordinates is complete", resourceType:"single", author:"Allan Koch Veiga"});
var criterionBId = ffub.registryCriterion({statement: "Coordinates is consistent", resourceType:"single", author:"Allan Koch Veiga"});

// Registring Enhancement
var enhancementId = ffub.registryEnhancement({description: "Recommend coordinates based on the municipality centroid", resourceType:"single", author:"Allan Koch Veiga"});

// Registring DQ Profile
var vie = [ieCoordinatesId, ieSpeciesNameId, ieCountryCodeId, ieMunicipalityId,ieLocalityId];
var measurementPolicy = [dimensionAId, dimensionBId, dimensionCId, dimensionDId];
var validationPolicy =[criterionId,criterionBId];
var amendmentPolicy =[enhancementId];
var profileId = ffub.registryProfile(useCaseId, vie, measurementPolicy, validationPolicy, amendmentPolicy, "Allan Koch Veiga");

// 2. REGISTRY - DQ Solutions

// Registring Specifications
var specificationAId = ffub.registrySpecification({description: "Is complete if latitude and latitude are different from null", author:"VertNet"});
var specificationBId = ffub.registrySpecification({description: "Is valid if latitude and latitude are different from null", author:"Allan Koch Veiga"});
var specificationCId = ffub.registrySpecification({description: "Recommend coordinates based on verbatim locality description using Google Maps API", author:"Allan Koch Veiga"});
var specificationDId = ffub.registrySpecification({description: "...", author:"Allan Koch Veiga"});
var specificationEId = ffub.registrySpecification({description: "Coordinates is inside the bounding box of the supplied country according to Google Maps", author:"VertNet"});
var specificationFId = ffub.registrySpecification({description: "Coordinates is inside the bounding box of the supplied country according to Google Maps", author:"VertNet"});

// Registring Mechanism
var mechanismId = ffub.registryMechanism({name:"BDQ-Toolkit", author:"Allan Koch Veiga", institutions:["BioComp","SiBBr"], sourceCode: "https://github.com/BioComp-USP/ffub", documentation:"https://github.com/BioComp-USP/ffub"});

// 3. REGISTRY - DQ Reports
// Registring Data Resource
console.log("Reading dataset...");   
console.time("Reading time");
// Read dataset (filePath, limit, delimiter, onReadRecord, onFinishedRead)
mechanism.readDatasetFromDwCFile(filePath, limit, delimiter, forEachRecord);
var print = 0;
var dataset = [];
var processRecord = async.queue(function(record, callback) {  
    var resourceType = "single";
    // Registry the record in FFUB
    var recordId = ffub.registryRecord(record);
    async.parallel([
        function completenessCoordinates(callback){
            // Registry Completeness DQ Measure for the record        
            var completenessResult = mechanism.measureCoordinatesCompletenessRecord(record);
            var measureIdCompleteness = ffub.registryMeasure(recordId, resourceType, dimensionAId, specificationAId, mechanismId, completenessResult);
            
            // Regsitry a Completeness DQ Validation for the record 
            var validationResultCompleteness = ffub.db.assertion.measure[measureIdCompleteness].result=="complete";
            ffub.registryValidation(recordId, resourceType, criterionId, specificationBId, mechanismId, validationResultCompleteness);
            return callback();
        },
        function consistencyGMaps(callback){
            mechanism.coordinatesConsistencyGMaps(record,function(result){                
                // Registry Completeness DQ Measure for the record
                var consistencyResult = result;
                var measureIdConsistency = ffub.registryMeasure(recordId, resourceType, dimensionCId, specificationEId, mechanismId, consistencyResult);
                
                // Registry a Completeness DQ Validation for the record 
                var validationResultConsistency = ffub.db.assertion.measure[measureIdConsistency].result=="consistent";
                ffub.registryValidation(recordId, resourceType, criterionBId, specificationBId, mechanismId, validationResultConsistency);
                                
                callback();
            });  
        },
        function consistencyRecommendGMaps(callback){
            // Regsitry a DQ Amendment for the record             
            if((!record.decimalLatitude || !record.decimalLongitude) && ((record.municipality && String(record.municipality).trim().length>0) || (record.locality && String(record.locality ).trim().length>0))){
                mechanism.recommendCoordinatesSingleFromMunicipality(record,function(amendmentResult){                    
                    ffub.registryAmendment(recordId, resourceType, enhancementId, specificationCId, mechanismId, amendmentResult);
                    ffub.printDQReport(record.id, profileId);
                    callback();
                });                                    
            } else callback();
        }
    ],function done(){
        callback();
    });            
    }, 1);

// Generate assertions and reports for rcords
function forEachRecord (record){      
    // Regsitry a DQ Measure for the record         
    processRecord.push(record,function(err){
        ffub.printDQReport(record.id, profileId);
    });   
    dataset.push(record) 
}

processRecord.drain = function(err) {    
    forEntireDataset(dataset)
};

// Generate assertions and report for dataset
function forEntireDataset (dataset){
    console.timeEnd("Reading time");
    console.time("Dataset report time");
    var resourceType = "multi";
    
    // Registry the dataset in FFUB
    var datasetId = ffub.registryDataset(dataset);     

    // Regsitry a DQ Measure for the dataset 
    var measureResult = mechanism.measureCoordinatesCompletenessDataset(dataset,dimensionAId);    
    ffub.registryMeasure(datasetId, resourceType, dimensionBId, specificationDId, mechanismId, measureResult);
    // Regsitry a DQ Measure for the dataset 
    var measureResultConsistency = mechanism.measureCoordinatesConsistencyDataset(dataset,dimensionCId);    
    ffub.registryMeasure(datasetId, resourceType, dimensionDId, specificationFId, mechanismId, measureResultConsistency);
        
    ffub.printDQReport(datasetId, profileId);
    console.timeEnd("Dataset report time");    
}
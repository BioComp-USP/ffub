// Load NodeJS modules
var Toolkit = require("./bdq-toolkit");
var FFUB = require("./index");

// Instance of of FFUB
var ffub = new FFUB();
// Instance of mechanism that will generates assertions results
var mechanism = new Toolkit(ffub);


// Params to read the Data Resource file 
var filePath = 'occurrence.txt', limit = 1000, delimiter = "\t";

// 1. REGISTRY - DQ Needs

// Registring Use Case
var useCaseId = ffub.registryUseCase({name: "A very simple example", author:"Allan Koch Veiga"});

// Registring Information Element
var ieCoordinatesId = ffub.registryIE({name: "Coordinates", dwc:["decimalLatitude", "decimalLongitude", "geodeticDatum"], author:"Allan Koch Veiga"});
var ieSpeciesNameId = ffub.registryIE({name: "Species name", dwc:["scientificName"], author:"Allan Koch Veiga"});

// Registring Dimension
var dimensionAId = ffub.registryDimension({name: "Coordinates completeness", resourceType:"single", ieId: ieCoordinatesId, dimension:"completeness", description:"Latitude and longitude supplied is complete", author:"Allan Koch Veiga"});
var dimensionBId = ffub.registryDimension({name: "Coordinates completeness", resourceType:"multi", ieId: ieCoordinatesId,  dimension:"completeness", description:"Proportion of latitude and longitude supplied", author:"Allan Koch Veiga"});

// Registring Criterion
var criterionId = ffub.registryCriterion({statement: "Coordinates is complete", resourceType:"single", author:"Allan Koch Veiga"});

// Registring Enhancement
var enhancementId = ffub.registryEnhancement({description: "Recommend coordinates based on the municipality centroid", resourceType:"single", author:"Allan Koch Veiga"});

// Registring DQ Profile
var vie = [ieCoordinatesId, ieSpeciesNameId];
var measurementPolicy = [dimensionAId, dimensionBId];
var validationPolicy =[criterionId];
var amendmentPolicy =[enhancementId];
var profileId = ffub.registryProfile(useCaseId, vie, measurementPolicy, validationPolicy, amendmentPolicy, "Allan Koch Veiga");

// 2. REGISTRY - DQ Solutions

// Registring Specifications
var specificationAId = ffub.registrySpecification({description: "Is complete if latitude and latitude are different from null", author:"Allan Koch Veiga"});
var specificationBId = ffub.registrySpecification({description: "Is valid if latitude and latitude are different from null", author:"Allan Koch Veiga"});
var specificationCId = ffub.registrySpecification({description: "Recommend coordinates based on verbatim locality description using Google Maps API", author:"Allan Koch Veiga"});
var specificationDId = ffub.registrySpecification({description: "...", author:"Allan Koch Veiga"});

// Registring Mechanism
var mechanismId = ffub.registryMechanism({name:"BDQ-Toolkit", author:"Allan Koch Veiga", institutions:["BioComp","SiBBr"], sourceCode: "https://github.com/BioComp-USP/ffub", documentation:"https://github.com/BioComp-USP/ffub"});

// 3. REGISTRY - DQ Reports
// Registring Data Resource
console.log("Reading dataset...");   
console.time("Reading time");
// Read dataset (filePath, limit, delimiter, onReadRecord, onFinishedRead)
mechanism.readDatasetFromDwCFile(filePath, limit, delimiter, forEachRecord, forEntireDataset);
var print = 0;

// Generate assertions and reports for rcords
function forEachRecord (record){  
    var resourceType = "single";
    
    // Registry the record in FFUB
    var recordId = ffub.registryRecord(record);
    
    // Regsitry a DQ Measure for the record 
    var measureResult = mechanism.measureCoordinatesCompletenessSingle(record);    
    var measureId = ffub.registryMeasure(recordId, resourceType, dimensionAId, specificationAId, mechanismId, measureResult);

    // Regsitry a DQ Validation for the record 
    var validationResult = mechanism.validateCoordinatesCompletenessSingleFromMeasureId(measureId);
    var validationId = ffub.registryValidation(recordId, resourceType, criterionId, specificationBId, mechanismId, validationResult);

    // Regsitry a DQ Amendment for the record 
    var amendmentResult = mechanism.recommendCoordinatesSingleFromVerbatimLocality(record);
    if(!validationResult)
        var amendmentId = ffub.registryAmendment(recordId, resourceType, enhancementId, specificationCId, mechanismId, amendmentResult);
    
    if(print<10){        
        ffub.printDQReport(record.id, profileId);
    }
    print++;    
}

// Generate assertions and report for dataset
function forEntireDataset (dataset){
    console.timeEnd("Reading time");
    console.time("Dataset report time");
    var resourceType = "multi";
    
    // Registry the dataset in FFUB
    var datasetId = ffub.registryDataset(dataset);     

    // Regsitry a DQ Measure for the dataset 
    var measureResult = mechanism.measureCoordinatesCompletenessDataset(dataset,dimensionAId);    
    var measureId = ffub.registryMeasure(datasetId, resourceType, dimensionBId, specificationDId, mechanismId, measureResult);
        
    ffub.printDQReport(datasetId, profileId);
    console.timeEnd("Dataset report time");    
}
var hash_ = require('object-hash');
var uuidv4 = require('uuid/v4');
var FFUB = function(){
    this.db = {};    
    // DQ Needs Class
    this.db.useCase = {};
    this.db.ie = {};
    this.db.dimension = {};
    this.db.criterion = {};
    this.db.enhancement = {};
    this.db.profile = {};

    // DQ Solutions Class
    this.db.mechanism = {};
    this.db.specification = {};

    // DQ Reports Class
    this.db.dataResource = {multi:{},single:{}};
    this.db.assertion = {measure:{},validation:{},amendment:{}};
}
// DQ Needs
FFUB.prototype.registryUseCase = function(data,id=null){
    var useCase = data;
    useCase.id = id?id:uuidv4();
    this.db.useCase[useCase.id] = useCase;
    return useCase.id;
}
FFUB.prototype.registryIE = function(data,id=null){
    var ie = data;
    ie.id = id?id:uuidv4();
    this.db.ie[ie.id] = ie;
    return ie.id;
}
FFUB.prototype.registryDimension = function(data,id=null){
    var dimension = data;
    dimension.id = id?id:uuidv4();
    this.db.dimension[dimension.id] = dimension;
    return dimension.id;
}
FFUB.prototype.registryCriterion = function(data,id=null){ 
    var criterion = data;
    criterion.id = id?id:uuidv4();
    this.db.criterion[criterion.id] = criterion;
    return criterion.id;
}
FFUB.prototype.registryEnhancement = function(data,id=null){
    var enhancement = data;
    enhancement.id = id?id:uuidv4();
    this.db.enhancement[enhancement.id] = enhancement;
    return enhancement.id;
}
FFUB.prototype.registryProfile = function(useCaseId, vie=[], measurementPolicy=[], validationPolicy=[], amendmentPolicy=[], author, id=null){
    var profile = {
            useCaseId: useCaseId,
            vie: vie,
            measurementPolicy:measurementPolicy,
            validationPolicy:validationPolicy,
            amendmentPolicy:amendmentPolicy,
            author: author,
            modified: new Date()
        };
    profile.id = id?id:uuidv4();
    this.db.profile[profile.id] = profile;
    return profile.id;
}
// DQ Solutions
FFUB.prototype.registrySpecification = function(data,id=null){
    var specification = data;
    specification.id = id?id:uuidv4();
    this.db.specification[specification.id] = specification;
    return specification.id;
}
FFUB.prototype.registryMechanism = function(data,id=null){
    var mechanism = data;
    mechanism.id = id?id:uuidv4();    
    this.db.mechanism[mechanism.id] = mechanism;
    return mechanism.id;
}
// DQ Report
FFUB.prototype.registryRecord = function(data,id=null){
    var self = this;
    var hash = hash_(data);
    if(!id) var id = data.id?data.id:uuidv4();
    self.db.dataResource.single[id] = {};
    self.db.dataResource.single[id].id = id;    
    self.db.dataResource.single[id].hash = hash_(data);    
    self.db.dataResource.single[id].data = data;        
    return id;
}
FFUB.prototype.registryDataset = function(data,persistData=false,id=null){
    var self = this;
    var hash = hash_(data);
    var id = id?id:uuidv4();
    self.db.dataResource.multi[id] = {};
    self.db.dataResource.multi[id].id = id;    
    self.db.dataResource.multi[id].hash = hash_(data);   
    if(persistData)
        self.db.dataResource.multi[id].data = data;        
    return id;
}
FFUB.prototype.registryMeasure = function(dataResourceId,resourceType,dimensionId,specificationId,mechanismId,result, id=null){    
    var assertion = {};
    assertion.id = id?id:uuidv4();
    assertion.modified = new Date();
    assertion.dataResourceId = dataResourceId;
    assertion.dimensionId = dimensionId;
    assertion.specificationId = specificationId;
    assertion.mechanismId = mechanismId;
    assertion.result = result;
    this.db.assertion.measure[assertion.id] = assertion;    
    return assertion.id;
}
FFUB.prototype.registryValidation = function(dataResourceId,resourceType,criterionId,specificationId,mechanismId,result, id=null){    
    var assertion = {};
    assertion.id = id?id:uuidv4();
    assertion.modified = new Date();
    assertion.dataResourceId = dataResourceId;
    assertion.criterionId = criterionId;
    assertion.specificationId = specificationId;
    assertion.mechanismId = mechanismId;
    assertion.result = result;
    this.db.assertion.validation[assertion.id] = assertion;   
    return assertion.id; 
}
FFUB.prototype.registryAmendment = function(dataResourceId,resourceType,enhancementId,specificationId,mechanismId,result, id=null){    
    var assertion = {};
    assertion.id = id?id:uuidv4();
    assertion.modified = new Date();
    assertion.dataResourceId = dataResourceId;
    assertion.enhancementId = enhancementId;
    assertion.specificationId = specificationId;
    assertion.mechanismId = mechanismId;
    assertion.result = result;
    this.db.assertion.amendment[assertion.id] = assertion;  
    return assertion.id;  
}

FFUB.prototype.retrieveMeasuresByDataResourceId = function(dataResourceId,dimensionId=null){    
    var self = this;
    var rs = [];
    Object.keys(self.db.assertion.measure).forEach(function(assertionId){
        if(self.db.assertion.measure[assertionId].dataResourceId==dataResourceId){
            if(dimensionId && dimensionId == self.db.assertion.measure[assertionId].dimensionId)
                rs.push(self.db.assertion.measure[assertionId]);            
        }
    });
    return rs;
}
FFUB.prototype.retrieveValidationByDataResourceId = function(dataResourceId,criterionId=null){    
    var self = this;
    var rs = [];
    Object.keys(self.db.assertion.validation).forEach(function(assertionId){
        if(self.db.assertion.validation[assertionId].dataResourceId==dataResourceId){
            if(criterionId && criterionId == self.db.assertion.validation[assertionId].criterionId)
                rs.push(self.db.assertion.validation[assertionId]);            
        }
    });
    return rs;
}
FFUB.prototype.retrieveAmendmentByDataResourceId = function(dataResourceId,enahncementId=null){    
    var self = this;
    var rs = [];
    Object.keys(self.db.assertion.amendment).forEach(function(assertionId){        
        if(self.db.assertion.amendment[assertionId].dataResourceId==dataResourceId){            
            if(enahncementId && enahncementId == self.db.assertion.amendment[assertionId].enhancementId)
                rs.push(self.db.assertion.amendment[assertionId]);            
        }
    });
    return rs;
}

FFUB.prototype.printDQMeasure = function(id){
    var self = this;    
    var assertion = this.db.assertion.measure[id];
    var dimension = self.db.dimension[assertion.dimensionId].name;            
    var result = "\x1b[1m"+new String(assertion.result).toUpperCase()+"\x1b[0m";
    var specification = self.db.specification[assertion.specificationId].description;
    var mechanism = self.db.mechanism[assertion.mechanismId].name;
    console.log(`\t- ${dimension}: ${result}`);
    console.log(`\t\tSpecification: ${specification}`);
    console.log(`\t\tMechanism: ${mechanism}`);
}
FFUB.prototype.printDQValidation = function(id){
    var self = this;
    var assertion = this.db.assertion.validation[id];
    var criterion = self.db.criterion[assertion.criterionId].statement;
    var result = assertion.result?"\x1b[1m\x1b[32m COMPLIANT \x1b[0m":"\x1b[1m\x1b[31m NOT COMPLIANT \x1b[0m";
    var specification = self.db.specification[assertion.specificationId].description;
    var mechanism = self.db.mechanism[assertion.mechanismId].name;
    console.log(`\t- ${criterion}: ${result}`);
    console.log(`\t\tSpecification: ${specification}`);
    console.log(`\t\tMechanism: ${mechanism}`);
}
FFUB.prototype.printDQAmendment = function(id){
    var self = this;
    var assertion = this.db.assertion.amendment[id];
    var enhancement = self.db.enhancement[assertion.enhancementId].description;                        
    var result = assertion.result&&Object.keys(assertion.result).length>0?"\x1b[32m"+JSON.stringify(assertion.result)+"\x1b[0m":(assertion.result==null)?"\x1b[33mUNKNOW\x1b[0m":"\x1b[32m"+assertion.result+"\x1b[0m";
    var specification = self.db.specification[assertion.specificationId].description;
    var mechanism = self.db.mechanism[assertion.mechanismId].name;
    console.log(`\t- ${enhancement}: ${result}`);
    console.log(`\t\tSpecification: ${specification}`);
    console.log(`\t\tMechanism: ${mechanism}`);
}
FFUB.prototype.printDQReport = function(dataResourceId, profileId){
    var self = this;
    var profile = self.db.profile[profileId];    
    var resourceType = self.db.dataResource.single[dataResourceId]?"single":self.db.dataResource.multi[dataResourceId]?"multi":null;    
    var dataResource = self.db.dataResource[resourceType][dataResourceId];  
    // Head
    console.log("\n");  
    console.log(`\x1b[36m\x1b[1m===== DQ REPORT FOR A ${resourceType.toUpperCase()}-RECORD DATA RESOURCE =====\x1b[0m`);
    
    // Use Case
    console.log("\n \x1b[4mUse Case\x1b[0m:",self.db.useCase[profile.useCaseId].name);    

    // Data resource
    console.log("\n \x1b[4mData Resource\x1b[0m:");    
    console.log(`\t- ID: ${dataResourceId}`);
    if(resourceType=="single")
        profile.vie.forEach(function(ieId){
            self.db.ie[ieId].dwc.forEach(function(dwc){
                console.log(`\t- ${dwc}: ${dataResource.data[dwc]}`);
            });
        });

    // DQ Measures
    var auxPrint = true;        
    profile.measurementPolicy.forEach(function(dimensionId){        
        if(self.db.dimension[dimensionId].resourceType==resourceType){            
            var assertion = self.retrieveMeasuresByDataResourceId(dataResourceId,dimensionId)[0];
            if(assertion && assertion.id){
                if(auxPrint)
                    console.log("\n \x1b[4mDQ Measures\x1b[0m:");
                self.printDQMeasure(assertion.id);
                auxPrint=false;
            }
        }            
    });

    // DQ Validations
    auxPrint = true;    
    profile.validationPolicy.forEach(function(criterionId){
        if(self.db.criterion[criterionId].resourceType==resourceType){            
            var assertion = self.retrieveValidationByDataResourceId(dataResourceId,criterionId)[0];
            if(assertion && assertion.id){
                if(auxPrint)
                    console.log("\n \x1b[4mDQ Validations\x1b[0m:");
                self.printDQValidation(assertion.id);    
                auxPrint=false;                   
            }
        }            
    }); 

    // DQ Amendments  
    auxPrint = true;   
    profile.amendmentPolicy.forEach(function(enhancementId){
        if(self.db.enhancement[enhancementId].resourceType==resourceType){
            var assertion = self.retrieveAmendmentByDataResourceId(dataResourceId,enhancementId)[0];            
            if(assertion && assertion.id){
                if(auxPrint)
                    console.log("\n \x1b[4mDQ Amendment\x1b[0m:");
                self.printDQAmendment(assertion.id);
                auxPrint=false;            
            }
        }            
    });       
    console.log("\n");             
}
module.exports = FFUB;
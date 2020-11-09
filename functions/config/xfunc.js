const XLSX = require('xlsx')
const slug = require("slug")
const {BigQuery} = require('@google-cloud/bigquery')



module.exports = {
    download: function(file,sheetName,bucket) {
        return new Promise(async (resolve, reject) => {
          try {
            const buffer = await bucket.file(file).download()
            const workbook = XLSX.read(buffer[0], {type: 'buffer'})
            const worksheet = workbook.Sheets[sheetName]
            resolve(XLSX.utils.sheet_to_json(worksheet))
          } catch (err) {
            reject(err)
          }
        })
    },

    ReadFile: function(fileName,sheetName,bucket)
    {
        return new Promise( resolve =>{
            const bucketFile = bucket.file(fileName); 
            let buffers = []
            var fileStream = bucketFile.createReadStream()
            fileStream.on('data',function(data) {
                buffers.push(data)
            })
            fileStream.on('end', function(){
                var buffer = Buffer.concat(buffers);
                var workbook = XLSX.read(buffer)
                var worksheet
                if(sheetName === 0)
                    worksheet = workbook.Sheets[workbook.SheetNames[0]]
                else
                    worksheet = workbook.Sheets[sheetName]
                resolve(XLSX.utils.sheet_to_json(worksheet))
            })
        })
    },

    FixKeys: function(item,itemList)
    {
        let keyFixer
        let row={}
        for(key in item)
        {
            keyFixer = slug(key,{lower : true,replacement:"_"})
                if(!itemList.includes(keyFixer))
                    if(item[key] !== "" && item[key] !== null)
                        row[keyFixer] = item[key]
           
        }
        return row
    },
    FixKeysInclude: function(item,itemList)
    {
        let keyFixer
        let row={}
        for(key in item)
        {
            keyFixer = slug(key,{lower : true,replacement:"_"})
                if(itemList.includes(keyFixer))
                    if(item[key] !== "" && item[key] !== null)
                        row[keyFixer] = item[key]
           
        }
        return row
    },
    SaveToBucket: function(xlData,fileName,bucket)
    {
        return new Promise(resolve =>{
            var stringedJSON = ""
            var file = bucket.file(fileName+".json")

            xlData.map(item => { return stringedJSON += JSON.stringify(item) + "\n"})
            var buffer = Buffer.from(stringedJSON)
            
            resolve(file.save(buffer))
        })
    },
    DeleteFromBucket: function(fileName, bucket)
    {
        bucket.file(fileName).delete()
    },

    LoadToBQ: async function(rows, tableName,del,counter)
    {
        try{
            let bigquery = new BigQuery()
            let res
            if (del)
            {
                res = await bigquery.query({ query: 'DELETE FROM `ogit-osds-dev.reporting.'+tableName+'` WHERE true' })
            }
            res = await bigquery
            .dataset("reporting")
            .table(tableName)
            .insert(rows)
            
            console.log(`-------------Uploaded to ${tableName} ${counter}`)
        }
        catch(error)
        {
            console.log(error)
        }
    },
    LoadFromFile: async function(fileName,tableName, bucket,del,callback)
    {
        let fileExists = false
        const bigquery = new BigQuery();
        
        file = await bucket.file(fileName+".json")
        fileExists = await file.exists()
        let res
        if (del)
        {
            res = await bigquery.query({ query: 'DELETE FROM `ogit-osds-dev.reporting.'+tableName+'` WHERE true' })
        }
        
        res = await bigquery
        .dataset("reporting")
        .table(tableName)
        .load(file)
        

        console.log(`----------Finished ${fileName}`)
        callback(fileName+".json", bucket)
        
    },
    BatchLoad: function(batchSize,data,tableName)
    {
      
        let counter = 0
        for(i=0;i<data.length;i+=batchSize)
        {
            counter++
            dataSlice = data.slice(i,i+batchSize)
            this.LoadToBQ(dataSlice,tableName,false,counter)
        }   
        
    },
   
}







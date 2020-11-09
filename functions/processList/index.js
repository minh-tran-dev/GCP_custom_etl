const admin = require('firebase-admin')
const bucket = admin.storage().bucket('reporting-lists')
const config = require("../config/config.js")
const xfunc = require("../config/xfunc.js")


exports.handler = async (object) => {

  const file = object.name.toLowerCase()

  if (file.match(/\.[0-9a-z]+$/i)[0] === '.xlsx' || file.match(/\.[0-9a-z]+$/i)[0] === '.xlsm') {

    if (file.includes('fileName')) {

      const data = await xfunc.ReadFile(object.name,"sheetName",bucket)
      
      let rows = data.map(function(x){return xfunc.FixKeysInclude(x,config.validSeatData)})
      rows = await FixDataType(rows)

      //await xfunc.SaveToBucket(rows,object.name,bucket)
      //await xfunc.LoadFromFile(object.name,"sheatName",bucket,true,xfunc.DeleteFromBucket)
      
      await xfunc.LoadToBQ(rows,"tableName",true,0)
      
      
      return
    }

    if (file.includes('fileName')) {
      //-------- Taxonomy
      
      let data = await xfunc.ReadFile(object.name,"sheetName",bucket)
      let rows = data.map(function(x){return xfunc.FixKeysInclude(x,config.validSegments)})
      rows = rows.filter(item => { return Number.isInteger(item.adex_id)})
    
      console.log("Uploading data")
      //await xfunc.SaveToBucket(rows,object.name,bucket)
      //await xfunc.LoadFromFile(object.name,"tableName",bucket,true,xfunc.DeleteFromBucket)
      await xfunc.LoadToBQ(rows,"tableName",true,0)

      //--------- Extra file
    
      data = await xfunc.ReadFile("fileName","sheetName",bucket)
      rows = data.map(function(x){return xfunc.FixKeysInclude(x,config.validSegments)})
      rows = rows.filter(item => { return Number.isInteger(item.adex_id)})

     
      //await xfunc.SaveToBucket(rows,"tableName",bucket)
      //await xfunc.LoadFromFile("tableName","do_segments",bucket,false,xfunc.DeleteFromBucket)
      await xfunc.LoadToBQ(rows,"tableName",false,1)

    }

  } 
  else 
  {
    return console.log(`skipping file: ${file}`)
  }
}

function FixDataType(objList)
{
  return new Promise(resolve=>{
    let unique_db_id = {}
    let unique_array = []
    for(i = 0; i< objList.length;i++)
    {
      if(Number.isInteger(objList[i].seat_id))
        objList[i].seat_id = String(objList[i].seat_id)

      unique_db_id[objList[i].db_id] = objList[i]
    }

    for(i in unique_db_id)
    {
      unique_array.push(unique_db_id[i])
    }
    
    unique_array = unique_array.filter(obj => {return Object.keys(obj).length})
    resolve(unique_array)
  })
}





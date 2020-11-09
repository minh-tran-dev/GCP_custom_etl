const admin = require('firebase-admin')
const bucket = admin.storage().bucket('reporting-finance')
const bucket2 = admin.storage().bucket('reporting-lists')
const config = require("../config/config.js")
const xfunc = require("../config/xfunc.js")
const {getJsDateFromExcel} = require("excel-date-to-js");

exports.handler = async (object) => {

    const file = object.name.toLowerCase()
  
    if (file.match(/\.[0-9a-z]+$/i)[0] === '.xlsx' || file.match(/\.[0-9a-z]+$/i)[0] === '.xlsm') {
  
      if (file.includes('cube')) {
        let invalidList = ["exceptions"]

        const data = await xfunc.ReadFile(object.name,"Cube",bucket)
        let rows = data.map(function(x){return xfunc.FixKeys(x,invalidList)})
        rows = rows.filter(obj => {return obj.kampagne_kampagnentype === "CustomFilter"})
        rows = rows.map(cleanUpData)
        const idData = await xfunc.ReadFile("filename","SheetName",bucket2)
        rows = rows.map(function(x){return addOsdsId(x,idData)})
        console.log("splitSegments")
        rows = await splitSegments(rows)
        
        console.log("Generate File")
        await xfunc.SaveToBucket(rows,"tableName",bucket)
        console.log("Upload")
        xfunc.LoadFromFile("tableName","tableName",bucket,true,xfunc.DeleteFromBucket)
        //xfunc.LoadToBQ(rows,"",true,0)
      }
    }
}

function cleanUpData(obj)
{
  obj["date"] = getDate(obj)
  obj.kampagne_werbetreibender_nummer = parseInt(obj.kampagne_werbetreibender_nummer)
  if(Number.isNaN(obj.kampagne_werbetreibender_nummer))
    delete obj.kampagne_werbetreibender_nummer
  obj.kampagne_hauptagentur_nummer = parseInt(obj.kampagne_hauptagentur_nummer)
  if(Number.isNaN(obj.kampagne_hauptagentur_nummer))
    delete obj.kampagne_hauptagentur_nummer
  obj.kampagne_buchende_agentur_nummer = parseInt(obj.kampagne_buchende_agentur_nummer)
  if(Number.isNaN(obj.kampagne_buchende_agentur_nummer))
    delete obj.kampagne_buchende_agentur_nummer

  if(obj.hasOwnProperty("kampagne_erstellungsdatum"))
    obj.kampagne_erstellungsdatum = getXlDate(obj.kampagne_erstellungsdatum)
  if(obj.hasOwnProperty("kampagne_erstellungsdatum_angebot"))
    obj.kampagne_erstellungsdatum_angebot = getXlDate(obj.kampagne_erstellungsdatum_angebot)
  if(obj.hasOwnProperty("position_lieferung_von"))
    obj.position_lieferung_von = getXlDate(obj.position_lieferung_von)
  if(obj.hasOwnProperty("position_lieferung_bis"))
    obj.position_lieferung_bis = getXlDate(obj.position_lieferung_bis)

  let revShare = 17  
  
  obj.agenturnetto_nach_skonto_und_arv_n31 = Number(obj.agenturnetto_nach_skonto_und_arv_n31).toFixed(4)
  obj["impressions"] = obj.kaufm_einheiten
  obj["umsatz_komplett"] = obj.agenturnetto_nach_skonto_und_arv_n31
  

  delete obj.jahr
  delete obj.monat
  
  return obj
}

function splitSegments(objList)
{
  return new Promise(resolve =>{
    
    let tempList = []
    let revShare = 17
    let obj
    let segments 
    let segScnd 

    for ( i = 0; i < objList.length; i++)
    {  
      segments = getSegments(objList[i].position_targeting)
      segScnd = getSegments(objList[i].position_targeting_zielgruppen)
      segments = segments.concat(segScnd)
       
      if(segments.length > 0)
      {
        for (j = 0; j < segments.length; j++)
        {
          obj = {}
          obj = JSON.parse(JSON.stringify(objList[i]))
          obj["segment_id"] = segments[j]
          obj.agenturnetto_nach_skonto_und_arv_n31 = Number(objList[i].agenturnetto_nach_skonto_und_arv_n31 / segments.length).toFixed(4)

          obj.impressions = Math.trunc(objList[i].kaufm_einheiten / segments.length)
          if(j === 0)
            obj.impressions  += objList[i].kaufm_einheiten % segments.length
          tempList.push(obj)
        }
      }
      else 
        tempList.push(objList[i]) 
    }
    resolve(tempList)
  })
}

function getSegments(segList)
{
  if (typeof segList === 'string')
  {
    let regExp = /(target >|target2 >)\s(.*?)\s>/g
    let match = segList.match(regExp)
    
    if (match === null || match === 'undefined')
      return []
    else
    {
      match = match.map(function(x){return parseInt(x.split('>')[1])})
      return match
    }
  }
  else return []
}

function getDate(obj)
{
  return obj.jahr +"-"+ obj.monat +"-"+"01"
}

function getXlDate(xlDate)
{
  let temp = getJsDateFromExcel(xlDate)
  let month = temp.getMonth() +1
  return temp.getFullYear() +"-" + month + "-" +temp.getDate()
}





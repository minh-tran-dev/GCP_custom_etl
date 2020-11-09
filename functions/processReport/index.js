const XLSX = require('xlsx')
const {getJsDateFromExcel} = require("excel-date-to-js");

const admin = require('firebase-admin')
const bucket = admin.storage().bucket('reporting-do')

const reportData = require('../config/reportData.json'); 
const conversionRates = require("../config/conversionRates.json"); 
const exchanges= ["Example"]
const xfunc = require("../config/xfunc.js");
const { BatchLoad } = require('../config/xfunc.js');
var weekly;

exports.handler = async (object) => {

  if (object.name.match(/\.[0-9a-z]+$/i)[0] === '.xlsx') {
    //const snap = await db.collection('reports').where('name', '==', object.name).get()

    //if (snap.empty) {

      console.log(`processing file ${object.name}`)

      var data = await xfunc.ReadFile(object.name,0,bucket)
      
      LoadData(data, object.name)
     /*
      await db.collection('reports').add({
        name: object.name,
        processed: Date.now(),
      })

      return
    } */
  }

  //return console.log(`skipping file ${object.name}`)
}

async function LoadData(fileData, fileName)
{  
  if ( CheckFile(fileName))
  {
  
    var xlData = fileData
    
    var exchangeName = GetExchange(fileName);
    //clean up data & insert to table
    xlData = await FixJsonList(xlData,exchangeName)
    xlData = await SpecificFixes(xlData,exchangeName)
    let currencyExchangeRate = await GetCurrencyExchangeRate()
    xlData = await AddToJsonList(xlData,exchangeName,fileName, currencyExchangeRate)
    if(exchangeName === "TargetExtra")
    {
      seatBucket = admin.storage().bucket('bucketName')
      advertiserData = await xfunc.ReadFile("fileName","sheetName",seatBucket)
      xlData = xlData.map(function(x){return addAdvertiser(x,advertiserData)})    
    }
   
    //console.log("saving to bucket")
    //await xfunc.SaveToBucket(xlData,fileName,bucket)
   
    let tableName
    if (weekly)
      tableName = "do_weekly"
    else 
      tableName = "do_monthly"
    console.log("uploading")
    xfunc.BatchLoad(5000,xlData,tableName)
    //await xfunc.LoadFromFile(fileName,tableName,bucket,false,xfunc.DeleteFromBucket)
  }
  else
    console.log("File Format Error " + fileName );
}

function CheckFile(fileName)
{
  if (fileName.includes("fileName") && fileName.includes("Report") && fileName.match(/_/g).length === 2)
  {
    temp = fileName.split("-");
    weekly =  temp.length >= 2
    for(e in exchanges)
      if( fileName.toLowerCase().includes(e.toLowerCase()))
        return true;
  }
  return false;
}

function FixJsonList(xlData, exchangeName)
{
  //variable used to match with entry in reportData.json
  let reportType;
  if (weekly)
    reportType ="_weekly";
  else
    reportType = "_monthly";
  if(exchangeName === exchanges[0] || exchangeName === exchanges[2] || exchangeName === exchanges[5])
    reportType="";

  xlData = xlData.filter(item => { return Object.keys(item).length >= 8})

  return new Promise (resolve => {
    for ( i = 0; i < xlData.length; i++)
    {   
      //clean up data to match with model
      let jsonString = JSON.stringify(reportData[exchangeName+reportType])
      for (e in xlData[i])
      {
          if(jsonString.toLowerCase().includes(e.toLowerCase()))
          {
            let found = false
            for (key in reportData[exchangeName+reportType])
              if(reportData[exchangeName+reportType][key].toLowerCase() === e.toLowerCase())
              { //match keys to model, delete old keys
                if(key !== e)
                {
                  xlData[i][key] = xlData[i][e];
                  delete xlData[i][e];
                }
                found = true;
              } 
            //necessary to handle similiar named keys
            if(!found)
              delete xlData[i][e]
          } 
          //delete keys not in model
          else
            delete xlData[i][e];
      }    
    }
    resolve(xlData);
  })
}

function SpecificFixes(xlData, exchangeName)
{
  return new Promise(resolve => {
    for(i=0; i < xlData.length; i++)
    {
        switch(exchangeName)
        {
          // - [isolate segmentID]
          case exchanges[2] : xlData[i].segment_id = parseInt(xlData[i].segment_id.substring(5,xlData[i].segment_id.length));
            break;
          // - [seatID inbetween last paranthesis]
          case exchanges[3]   : if(!weekly)
                          {
                            let regExp = /\(([^)]*)\)[^(]*$/;
                            let match = regExp.exec(xlData[i].seat_id);
                            if (Array.isArray(match))
                              xlData[i].seat_id = match[match.length-1];
                            else 
                              console.log("array not found")
                          }    
            break; 
          // - [fix multiple segmentIDs]    
          case exchanges[1] : if (typeof xlData[i].segment_id === 'string' || xlData[i].segment_id instanceof String)
                              {
                                let temp = xlData[i].segment_id.split(",");
                                let obj = {}
                              
                                for (j = 0 ; j < temp.length;j++)
                                {
                                  obj = JSON.parse(JSON.stringify(xlData[i]));
                                  obj.segment_id = parseInt(temp[j]);
                                  if(obj.impressions === "undefined")
                                    obj.impressions = 0
                                  obj.impressions = Math.trunc(xlData[i].impressions / temp.length);
                                  obj.revenue_original = xlData[i].revenue_original / temp.length;
                                  if (j === 0)
                                    obj.impressions += xlData[i].impressions % temp.length;
                                  xlData.push(obj);
                            
                                }
                                delete xlData[i] 
                              }       
                              break;  
          case exchanges[5] : regExp = />\s(.*?)\s>/
                              match = regExp.exec(xlData[i].segment_id)  
                              xlData[i].segment_id = parseInt(match[1])
            break;
        } 
    }
    xlData = xlData.filter(item => {return item.impressions !== 0})
    xlData = xlData.filter(item => {return item.seat_id !== "undefined"});
    resolve(xlData);
  })
}

async function AddToJsonList(xlData, exchangeName, fileName, currencyExchangeRate)
{
  return new Promise(resolve => {
    let date = GetDate(fileName, weekly);
    for( i = 0; i < xlData.length; i++)
    {
        if(!weekly)
          xlData[i].date = date;
        else
        {
          xlData[i].date = getJsDateFromExcel(xlData[i].date)
          let month = xlData[i].date.getMonth()+1
          xlData[i].date = xlData[i].date.getFullYear()+"-"+month+"-"+xlData[i].date.getDate()
        }
        xlData[i].exchange = exchangeName;
        if(exchangeName === exchanges[1] ||exchangeName === exchanges[0] )
          xlData[i].revenue_euro = Number(xlData[i].revenue_original).toFixed(4);
        else
          xlData[i].revenue_euro = Number(xlData[i].revenue_original / currencyExchangeRate).toFixed(4);
        xlData[i].revenue_exchange = Number(xlData[i].revenue_euro / (1 - conversionRates[exchangeName])).toFixed(4);
       
        xlData[i].tkp = Number(xlData[i].revenue_euro * 1000 / xlData[i].impressions).toFixed(4);
        xlData[i].revenue_original = Number(xlData[i].revenue_original).toFixed(4);
        if(Number.isInteger(xlData[i].seat_id))
          xlData[i].seat_id = xlData[i].seat_id.toString();
        xlData[i].db_id = exchangeName + xlData[i].seat_id;

        if( typeof xlData[i].advertiser !== "undefined")
        { 
          if (isNaN(xlData[i].advertiser));
            xlData[i].advertiser = xlData[i].advertiser.toString();
        }
        
      
    }
    resolve(xlData);
    }); 
}

//naming of files significant
function GetDate(fileName, weekly)
{
  if (!weekly)
  {
    var dateStr = fileName.split("_");
    dateStr = dateStr[2]; 
    dateStr = dateStr.substring(0,dateStr.length - 5); 
    let year = dateStr.substring(dateStr.length-4, dateStr.length); 
    let month;
    let day = "01";
    let temp = dateStr.split(/[ ,]+/);
    temp = temp[0];
    switch (temp)
    {
      case "January" :  month = "01";
        break;
      case "February":  month = "02";
        break;
      case "March" :    month = "03";
        break;
      case "April" :    month = "04";
        break;
      case "May":       month = "05";
        break;
      case "June":      month = "06";
        break;
      case "July":      month = "07";
        break;
      case "August":    month = "08";
        break;
      case "September": month = "09";
        break;
      case "October":   month = "10";
        break;
      case "November":  month = "11";
        break;
      case "December":  month = "12";
        break;
    }
    
    return year +"-"+ month+"-"+ day
  }
}

function GetExchange(fileName)
{
  var exchange = fileName.split("_");
  exchange = exchange[1];
  exchange = exchange.substring(0,exchange.length - 7);
  exchange = exchange.replace(/ /g,"");
  return exchange;
}

function GetCurrencyExchangeRate()
{
  return new Promise((resolve) => {
    bucket.file("CurrencyExchangeRate.xlsx").download().then(currencyData =>{
      let workbook = XLSX.read(currencyData[0], {type : "buffer"});
      let worksheet = workbook.Sheets[workbook.SheetNames[0]];
      let data = XLSX.utils.sheet_to_json(worksheet);
      resolve(parseFloat(data[0].CurrencyExchangeRate));
      return null
    }) 
    .catch(error => { console.log(error)})
  })
}

function addAdvertiser(obj,advertiserList)
{
  let entry = advertiserData.find(item => item.db_id === obj.db_id)
  obj.advertiser = entry.Advertiser
  return obj
}

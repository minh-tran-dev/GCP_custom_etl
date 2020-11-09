const functions = require('firebase-functions')
const admin = require('firebase-admin')

admin.initializeApp()

const processReport = require('./processReport')
const processList = require('./processList')
const processFinance = require('./processFinance')

exports.processReport = functions.runWith({memory:"1GB"}).region('europe-west1').storage.bucket('reporting-do').object().onFinalize(processReport.handler)
exports.processList = functions.runWith({memory:"1GB"}).region('europe-west1').storage.bucket('reporting-lists').object().onFinalize(processList.handler)
exports.processFinance = functions.runWith({memory:"2GB"}).region('europe-west1').storage.bucket('reporting-finance').object().onFinalize(processFinance.handler)
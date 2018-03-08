/**
 * Created by jacky on 2017/2/4.
 */
'use strict';
var _ = require('lodash');
var util = require('util');
var async = require('async');
var mongoose = require('mongoose');
var VirtualDevice = require('./virtual-device').VirtualDevice;
var logger = require('./mlogger/mlogger');
var eventLevelConf = require('../event-level.json');
var Sequence = require('./sequence/sequence').Sequence;
var sequence = new Sequence("event");
var eventSchema = new mongoose.Schema({
  index: {type: Number},
  userUuid: {type: String},
  ownerUuid: {type: String},
  deviceUuid: {type: String},
  deviceName: {type: String},
  deviceType: {type: String},
  eventTag: {type: String},
  eventLevel: {type: Number},
  eventDescription: {type: String},
  handled: {type: Boolean, default: false},
  timestamp: {type: Date, default: Date.now}
});
eventSchema.pre('save', function (next) {
  var doc = this;
  // get the next sequence
  sequence.next(function (nextSeq) {
    logger.debug("nextSeq = " + nextSeq);
    doc.index = nextSeq;
    next();
  });
});
var OPERATION_SCHEMAS = {
  saveEvent: {
    "type": "object",
    "properties": {
      "userUuid": {"type": "string"},
      "ownerUuid": {"type": "string"},
      "deviceUuid": {"type": "string"},
      "deviceName": {"type": "string"},
      "deviceType": {"type": "string"},
      "eventTag": {"type": "string"},
      "eventLevel": {"type": "integer"},
      "eventDescription": {"type": "string"}
    },
    "required": ["userUuid", "ownerUuid", "deviceUuid", "deviceName", "deviceType", "eventTag", "eventLevel", "eventDescription"]
  },
  getEvent: {
    "type": "object",
    "properties": {
      "select": {
        "type": "array",
        "items": {
          "type": "string"
        }
      },
      "where": [
        {
          "type": "object",
          "properties": {
            "key": {"type": "string"},
            "value": {"type": "object"},
            "op": {
              "type": "string",
              "enum": [
                "eq",
                "gte",
                "gt",
                "lte",
                "lt",
                "ne",
                "in"
              ]
            }
          },
          "required": ["key", "value", "op"]
        }
      ],
      "between": [
        {
          "type": "object",
          "properties": {
            "key": {"type": "string"},
            "value": [
              {"type": "object"}
            ]
          },
          "required": ["key", "value"]
        }
      ],
      "limit": {"type": "integer"}
    }
  },
  getLatestEvent: {
    "type": "object",
    "properties": {
      "userUuid": {"type": "string"}
    },
    "required": ["userUuid"]
  }
};

function getNotificationKey(self, deviceInfo, callback) {
  async.waterfall(
    [
      function (innerCallback) {
        var message = {
          devices: self.configurator.getConfRandom("services.device_manager"),
          payload: {
            cmdName: "getDevice",
            cmdCode: "0003",
            parameters: {
              uuid: deviceInfo.userId
            }
          }
        };
        self.message(message, function (respMsg) {
          if (respMsg.payload.code !== 200) {
            innerCallback({
              errorId: respMsg.payload.code,
              errorMsg: respMsg.payload.message
            })
          }
          else {
            var notificationKey = null;
            var ownerInfo = respMsg.payload.data;
            logger.info(ownerInfo.settings);
            if (ownerInfo.settings) {
              for (var index = 0, len = ownerInfo.settings.length; index < len; ++index) {
                if (ownerInfo.settings[index].name === "gcm_token") {
                  notificationKey = ownerInfo.settings[index].value;
                  logger.info("notificationKey:[" + notificationKey + "]");
                  break;
                }
              }
            }
            innerCallback(null, notificationKey);
          }
        });
      }
    ],
    function (error, result) {
      if (error) {
        callback(error);
      }
      callback(null, result);
    }
  );
}

function EventCenter(conx, uuid, token, configurator) {
  VirtualDevice.call(this, conx, uuid, token, configurator);
}

util.inherits(EventCenter, VirtualDevice);

/**
 * 远程RPC回调函数
 * @callback onMessage~saveEvent
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "retCode":{string},
 *          "description":{string},
 *          "data":{object}
 *      }
 * }
 */
/**
 * 保存事件
 * @param {object} message:输入消息
 * @param {onMessage~saveEvent} peerCallback: 远程RPC回调
 * */
EventCenter.prototype.saveEvent = function (message, peerCallback) {
  var self = this;
  logger.warn(message);
  var responseMessage = {retCode: 200, description: "Success.", data: {}};
  self.messageValidate(message, OPERATION_SCHEMAS.saveEvent, function (error) {
    if (error) {
      responseMessage = error;
      peerCallback(error);
    }
    else {
      var eventDoc = message;
      if (eventDoc.eventLevel && eventDoc.eventLevel >= eventLevelConf.pushLevel) {
        //push notification
        var msg = {
          devices: self.configurator.getConfRandom("services.device_manager"),
          payload: {
            cmdName: "getDevice",
            cmdCode: "0003",
            parameters: {
              uuid: eventDoc.deviceUuid
            }
          }
        };
        self.message(msg, function (response) {
          if (response.retCode === 200) {
            var deviceInfo = response.data;
            if (util.isArray(response.data)) {
              deviceInfo = response.data[0];
            }
            if (!util.isNullOrUndefined(deviceInfo)) {
              getNotificationKey(self, self.deviceInfo, function (error, notificationKey) {
                if (error) {
                  logger.error(error.errorId, error.errorMsg);
                }
                else if (notificationKey) {
                  var messageInfo = {
                    devices: self.configurator.getConfRandom("services.notification"),
                    payload: {
                      cmdName: "sendMessage",
                      cmdCode: "0001",
                      parameters: {
                        target: {
                          registrationIds: [notificationKey]
                        },
                        payload: {
                          notification: {
                            title: "M-Cloud",
                            body: eventDoc.eventDescription,
                            icon: "ic_launcher"
                          }
                        }
                      }
                    }
                  };
                  self.message(messageInfo, function (respMsg) {
                    if (respMsg.payload.code !== 200) {
                      logger.error(respMsg.payload.code, respMsg.payload.message)
                    }
                    else {
                      logger.info("Push notification success:" + respMsg.payload.data)
                    }
                  });
                }
                else {
                  logger.info("user[" + self.deviceInfo.userId + "] have not GCM token.")
                }
              });
            }
          }
        });
      }
      //save event to db
      var db = mongoose.createConnection(self.configurator.getConf("meshblu_server.db_url"));
      db.once('error', function (error) {//200005
        var logError = {errorId: 200005, errorMsg: JSON.stringify(error)};
        logger.error(200005, error);
        responseMessage.code = logError.errorId;
        responseMessage.message = logError.errorMsg;
        if (peerCallback && _.isFunction(peerCallback)) {
          peerCallback({payload: responseMessage});
        }
        db.close();
      });

      db.once('open', function () {
        var eventModel = db.model('events', eventSchema);
        var mongooseEntity = new eventModel(eventDoc);
        mongooseEntity.save(function (error, res) {
          if (error) {
            logger.error(212000, error);
            responseMessage.code = 212000;
            responseMessage.message = error;
          }
          else {
            //logger.debug(res);
          }
          if (peerCallback && _.isFunction(peerCallback)) {
            peerCallback(responseMessage);
          }
          db.close();
        });
      });
    }
  });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~getEvent
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "retCode":{string},
 *          "description":{string},
 *          "data":{object}
 *      }
 * }
 */
/**
 * 事件查询
 * @param {object} message:输入消息
 * @param {onMessage~getEvent} peerCallback: 远程RPC回调
 * */
EventCenter.prototype.getEvent = function (message, peerCallback) {
  var self = this;
  var requestConditions = message;
  logger.debug(requestConditions);
  var responseMessage = {retCode: 200, description: "Success.", data: []};
  self.messageValidate(message, OPERATION_SCHEMAS.getEvent, function (error) {
    if (error) {
      responseMessage = error;
      peerCallback(error);
    }
    else {
      var db = mongoose.createConnection(self.configurator.getConf("meshblu_server.db_url"));
      db.once('error', function (error) {//200005
        var logError = {errorId: 200005, errorMsg: JSON.stringify(error)};
        logger.error(200005, error);
        responseMessage.code = logError.errorId;
        responseMessage.message = logError.errorMsg;
        if (peerCallback && _.isFunction(peerCallback)) {
          peerCallback({payload: responseMessage});
        }
      });

      db.once('open', function () {
        var eventModel = db.model('events', eventSchema);
        var sort = {index: "desc"};
        var queryCondition = {};
        if (requestConditions.where) {
          for (var index1 = 0, len1 = requestConditions.where.length; index1 < len1; index1++) {
            if (requestConditions.where[index1].op === "eq") {
              queryCondition[requestConditions.where[index1].key] = requestConditions.where[index1].value;
            }
          }
        }
        /* if (self.deviceInfo.type.id === '060A08000000') {
             queryCondition.userUuid = self.deviceUuid;
         }
         else {
             queryCondition.deviceUuid = self.deviceUuid;
         }*/
        logger.debug(queryCondition);
        var query = eventModel.find(queryCondition);
        if (requestConditions.where) {
          for (var index3 = 0, len3 = requestConditions.where.length; index3 < len3; index3++) {
            if (requestConditions.where[index3].op === "gte") {
              logger.debug(requestConditions);
              sort.index = "asc";
              query = query.where(requestConditions.where[index3].key).gte(requestConditions.where[index3].value);
            }
            else if (requestConditions.where[index3].op === "gt") {
              logger.debug(requestConditions);
              sort.index = "asc";
              query = query.where(requestConditions.where[index3].key).gt(requestConditions.where[index3].value);
            }
            else if (requestConditions.where[index3].op === "lte") {
              logger.debug(requestConditions);
              sort.index = "desc";
              query = query.where(requestConditions.where[index3].key).lte(requestConditions.where[index3].value);
            }
            else if (requestConditions.where[index3].op === "lt") {
              logger.debug(requestConditions);
              sort.index = "desc";
              query = query.where(requestConditions.where[index3].key).lt(requestConditions.where[index3].value);
            }
            else if (requestConditions.where[index3].op === "ne") {
              logger.debug(requestConditions);
              query = query.where(requestConditions.where[index3].key).ne(requestConditions.where[index3].value);
            }
            else if (requestConditions.where[index3].op === "in") {
              logger.debug(requestConditions);
              query = query.where(requestConditions.where[index3].key).in(requestConditions.where[index3].value);
            }
            else if (requestConditions.where[index3].op === "eq") {

            }
            else {
              logger.error(212000, "Invalid operation:[" + requestConditions.where[index3].op + "]")
            }
          }
        }
        if (requestConditions.between) {
          for (var index2 = 0, len2 = requestConditions.between.length; index2 < len2; index2++) {
            query = query.where(requestConditions.between[index2].key)
              .gte(requestConditions.between[index2].value[0])
              .lte(requestConditions.between[index2].value[1]);
          }
        }
        if (!requestConditions.limit) {
          requestConditions.limit = 10;
        }
        logger.debug("requestConditions.limit = " + requestConditions.limit);
        query = query.sort(sort).limit(requestConditions.limit);
        if (requestConditions.select && requestConditions.select.length > 0) {
          var selectStr = '';
          for (var index = 0, len = requestConditions.select.length; index < len; ++index) {
            if (index === 0) {
              selectStr += requestConditions.select[index];
            }
            else {
              selectStr += ' ' + requestConditions.select[index];
            }
          }
          query = query.select(selectStr);
        }
        query.exec(function (error, result) {
          if (error) {
            var logError = {errorId: 212000, errorMsg: JSON.stringify(error)};
            logger.error(212000, error);
            responseMessage.code = logError.errorId;
            responseMessage.message = logError.errorMsg;
          }
          else {
            responseMessage.data = [];
            logger.trace(result);
            if (sort.index === "asc") {
              for (var i = result.length - 1; i >= 0; --i) {
                responseMessage.data.push(JSON.parse(JSON.stringify(result[i]._doc)));
              }
            }
            else {
              for (var i1 = 0, len1 = result.length; i1 < len1; ++i1) {
                responseMessage.data.push(JSON.parse(JSON.stringify(result[i1]._doc)));
              }
            }
          }
          if (peerCallback && _.isFunction(peerCallback)) {
            peerCallback(responseMessage);
          }
          db.close();
        });
      });
    }
  });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~getLatestEvent
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "retCode":{string},
 *          "description":{string},
 *          "data":{object}
 *      }
 * }
 */
/**
 * 事件查询
 * @param {object} message:输入消息
 * @param {onMessage~getLatestEvent} peerCallback: 远程RPC回调
 * */
EventCenter.prototype.getLatestEvent = function (message, peerCallback) {
  var self = this;
  logger.debug(message);
  var responseMessage = {retCode: 200, description: "Success.", data: []};
  self.messageValidate(message, OPERATION_SCHEMAS.getLatestEvent, function (error) {
    if (error) {
      responseMessage = error;
      peerCallback(error);
    }
    else {
      async.waterfall([
        function (innerCallback) {
          var msg = {
            devices: self.configurator.getConfRandom("services.device_manager"),
            payload: {
              cmdName: "getDevice",
              cmdCode: "0003",
              parameters: {
                userId: message.userUuid
              }
            }
          };
          self.message(msg, function (response) {
            if (response.retCode === 200) {
              var deviceInfo = response.data;
              innerCallback(null, deviceInfo);
            } else {
              innerCallback(null, []);
            }
          });
        }
      ], function (error, devices) {
        if (util.isArray(devices) && devices.length > 0) {
          var db = mongoose.createConnection(self.configurator.getConf("meshblu_server.db_url"));
          db.once('error', function (error) {//200005
            var logError = {errorId: 200005, errorMsg: JSON.stringify(error)};
            logger.error(200005, error);
            responseMessage.code = logError.errorId;
            responseMessage.message = logError.errorMsg;
            if (peerCallback && _.isFunction(peerCallback)) {
              peerCallback({payload: responseMessage});
            }
          });

          db.once('open', function () {
            var eventModel = db.model('events', eventSchema);
            async.mapSeries(devices, function (device, callback) {
              var sort = {index: "desc"};
              var queryCondition = {
                "userUuid": device.userId,
                "deviceUuid": device.uuid
              };
              var query = eventModel.find(queryCondition);
              query = query.sort(sort).limit(1);
              query.exec(function (error, result) {
                if (error) {
                  callback(null, null);
                }
                else {
                  callback(null, result[0]);
                }
              });
            }, function (error, result) {
              for (var i = 0, len = result.length; i < len; ++i) {
                if (!util.isNullOrUndefined(result[i])) {
                  responseMessage.data.push(JSON.parse(JSON.stringify(result[i]._doc)));
                }
              }
              if (peerCallback && _.isFunction(peerCallback)) {
                peerCallback(responseMessage);
              }
              db.close();
            });
          });
        }
        else {
          if (peerCallback && _.isFunction(peerCallback)) {
            peerCallback(responseMessage);
          }
        }
      });
    }
  });
};

module.exports = {
  Service: EventCenter,
  OperationSchemas: OPERATION_SCHEMAS
};
var express = require('express');
const formidable = require('express-formidable');
var app = express();
var cors = require('cors');
var projectService = require('./project-service');
var userService = require('./user-service');
var imagineService = require('./imagine-service');
var deliverService = require('./deliver-service');
var commonService = require('./common-service');
var runservice = require('./run-service');
var bodyParser = require('body-parser');
var fs = require("fs");
var sql = require("mssql");
var Busboy = require('busboy');
var projectLogoFileName = '';
var uuidv1 = require('uuid/v1');

const multer = require('multer');
const path = require('path');
const api_helper = require('./team-api-helper');
var connection = require('./connection-file');
const helpers = require('./helpers');
var marketplace = require('./market-place.js');

// config for your database
var config = {
    user: 'ascendadmin',
    password: 'Dragon@2019',
    server: 'ascenddb.database.windows.net',
    database: 'AZDB-USCON-ASCEND-NPD',
    encrypt: true
};

app.use(cors());
app.use(bodyParser.json({ limit: '2mb', extended: true }));
app.use(express.urlencoded({ extended: false }));
/*
app.use('/projectdetailupload', formidable({
    keepExtensions: true,
    limit: 1024 * 1024 * 1024 * 500,
    defer: true
}));
*/
app.use(express.static(__dirname + '/public'));

const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, 'uploads/');
    },

    // By default, multer removes file extensions so let's add them back
    filename: function (req, file, cb) {
        cb(null, file.fieldname + '-' + Date.now() + path.extname(file.originalname));
    }
});

app.post('/projectdetailupload2', (req, res, next) => {

    console.log(req.fields);
    console.log(req.files);
    console.log(req.body);

    projectService.postprojectdetailupload(req.fields.data).then(
        (data) => {
            console.log('In projectdetailupload Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            return null;
        });

    // 'profile_pic' is the name of our file input field in the HTML form
    /*
    let upload = multer({ storage: storage, fileFilter: helpers.imageFilter }).single('profile_pic');

    upload(req, res, function(err) {
        // req.file contains information of uploaded file
        // req.body contains information of text fields, if there were any

        if (req.fileValidationError) {
            return res.send(req.fileValidationError);
        }
        else if (!req.file) {
            return res.send('Please select an image to upload');
        }
        else if (err instanceof multer.MulterError) {
            return res.send(err);
        }
        else if (err) {
            return res.send(err);
        }

        // Display uploaded image for user validation
        res.send(`You have uploaded this image: <hr/><img src="${req.file.path}" width="500"><hr /><a href="./">Upload another image</a>`);
    });*/
});

app.post('/projectdetailupload', (req, res) => {

    var busboy = new Busboy({
        headers: req.headers
    });

    var projectLogoFileName = '';

    busboy.on('file', function (fieldname, file, filename, encoding, mimetype) {
        //var saveTo = path.join('/home/site/wwwroot/uploads/' + uuidv1() + filename);
        var saveTo = uuidv1() + '_' +filename;
        projectLogoFileName = saveTo;
        console.log('Project Logo File Name 1: '+projectLogoFileName);
        //file.pipe(fs.createWriteStream(saveTo));

        file.on('data', function(data) {
            //console.log('File [' + fieldname + '] got ' + data.length + ' bytes');
            //console.log(data);
            projectService.uploadLogo(projectLogoFileName,data,data.length).then(
                (data) => {
                    console.log('In uploadLogo Code:');
                    console.log(data);
                    //res.send(data);
                }
            )
                .catch(function (err) {
                    console.log('In Return Upload Logo - Error:'+err);
                    //return null;
                });
          });
          //file.on('end', function() {
            //console.log('File [' + fieldname + '] Finished');
          //});

    });

    console.log('Project Logo File Name 2: '+projectLogoFileName);

    busboy.on('field', function (fieldname, val, fieldnameTruncated, valTruncated) {

        projectService.postprojectdetailupload(val,projectLogoFileName).then(
            (data) => {
                console.log('In postprojectdetailupload Code:');
                console.log(data);
                res.send(data);
            }
        )
            .catch(function (err) {
                console.log(err);
                return null;
            });
    });
    busboy.on('finish', function () {
        console.log('Done!');
    });
    return req.pipe(busboy);
});

app.post('/superUserList', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    projectService.postSuperUserList(req.body).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/membersupdate', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    //let projectId = req.params.projectId;
    projectService.postMembersupdate(req.body).then(
        (data) => {
            console.log('In postMembersupdate Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});
app.get('/marketplacefilter', function (req, res) {
    console.log('In marketplacefilter::::');
    marketplace.getMarketPlaceFilter().then(
        (data) => {
            console.log('In getMarketPlaceFilter Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/marketplacetools', function (req, res) {
    console.log('In marketplacefilter::::');
    marketplace.getMarketPlaceTools().then(
        (data) => {
            console.log('In getMarketPlaceTools Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});
app.get('/userlist', function (req, res) {
    console.log('In userlist::::');
    userService.getUserlistSource().then(
        (data) => {
            console.log('In getUserlistSource Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/superUserList', function (req, res) {
    console.log('In superUserList::::');
    projectService.getSuperUserListSource().then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/scopeDetailsPage', function (req, res) {
    console.log('In scopeDetailsPage::::');
    projectService.getScopeDetailsPagefun().then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/clientDetailsPage', function (req, res) {
    console.log('In clientDetailsPage::::');
    projectService.getClientDetailsPageSource().then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/projectDetailsPage', function (req, res) {
    console.log('In projectDetailsPage::::');
    projectService.getProjectDetailsPagefunc().then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/userInfo/:user', function (req, res) {

	console.log('In Test::::' + req.params.user);
	//var idToken = req.headers.authorization;
	console.log('idToken: ' + req.headers.authorization);
	console.log('request.headers.host :' + req.headers.host);
	var hostname = req.headers.host;
	//hostname = hostname.substr(0,9);
	//console.log('Final hostname : '+ hostname);
	commonService.GetAuthendication(null, req.params.user ,
									req.headers.authorization,
									connection.getconnection().createTeamsClientId,
									connection.getconnection().createTeamsClientSecret
									//,hostname
									).then( //(userroleid,emailid,req.headers.authorization,clientid,clientsecret,hostname)
        (data) => {
           // console.log('Hello:' + data);
			if (data == null)
				{
					  userService.getUserinfouser(req.params.user).then(
						(data) => {
							//console.log('In Main Code:');
							//console.log(data);
							if (data == null)
											{
							                	res.send('[{\"isAscendAdmin\":\"false\",\"projectInfo\":[],\"userId\":\"'+req.params.user+'\" }]');
							                }
							                else{
							                    res.send(data);
                							}
						}
					)
						.catch(function (err) {
							console.log(err);
							return null;
						});
                }
            else
				{
                  res.send(data);

                }
		}
		)
		.catch(function (err) {
							console.log(err);
							conn.close();
							return null;
						});

});

/*app.post('/publish/email/:projectId', function (req, res) {
    console.log('In Test::::' + req.params.projectId);
    projectService.postemailservice(req.params.projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});*/

app.get('/project/summary/:projectid', function (req, res) {
    console.log('In Test::::' + req.params.projectid);
    projectService.getProjectsummaryid(req.params.projectid).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/project/documents/:projectid', function (req, res) {
    console.log('In Test::::' + req.params.projectid);
    projectService.getProjectdocumentsid(req.params.projectid).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/getClientLogo/:projectId', function (req, res) {
    console.log('In Azure Call - getClientLogoprojectId :' + req.params.projectId);
    commonService.getClientLogoprojectId(req.params.projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            var readStream = fs.createReadStream(data);
            // This will wait until we know the readable stream is actually valid before piping
            readStream.on('open', function () {
                // This just pipes the read stream to the response object (which goes to the client)
                readStream.pipe(res);
            });
            //res.send(data);
        }
    )
        .catch(function (err) {
            console.log('In Azure Error - getClientLogoprojectId');
            console.log(err);

            conn.close();
            return null;
        });
});

app.get('/projectmembers/:project', function (req, res) {
    console.log('In Test::::' + req.params.project);
    userService.getProjectMembersProject(req.params.project).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/projectdetails/:project', function (req, res) {
    console.log('In Test::::' + req.params.project);
    projectService.getProjectDetailsProject(req.params.project).then(
        (data) => {
            console.log('In projectdetails Code:');
            console.log(data);
            if(data)
            res.send(data);

            if(!data)
            res.send('null')
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

/*
app.get('/architect/filterregionv5/:userroleid', function (req, res) {
    console.log('In Test::::' + req.params.userroleid);
    commonService.getFilterRegion(req.params.userroleid).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/architect/filterbusinessv5/:userroleid', function (req, res) {
    console.log('Input:' + req.params.userroleid);

    commonService.getFilterBusiness(
        req.params.userroleid
    ).then(
        (data) => {
            console.log('In getFilterBusiness Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/architect/filterindustryv5/:userroleid', function (req, res) {
    console.log('Input:' + req.params.userroleid);

    commonService.getFilterIndustry(
        req.params.userroleid
    ).then(
        (data) => {
            console.log('In getFilterIndustry Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

*/


app.get('/architect/filterregionv5/:userroleid/:tabname', function (req, res) {
    console.log('In Test::::' + req.params.userroleid);
	    console.log('In Test::::' + req.params.tabname);

    commonService.getFilterRegion(req.params.userroleid,req.params.tabname).then(
        (data) => {
            console.log('In getFilterRegion Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/architect/filterbusinessv5/:userroleid/:tabname', function (req, res) {
    console.log('Input:' + req.params.userroleid);
	    console.log('In Test::::' + req.params.tabname);

    commonService.getFilterBusiness(
        req.params.userroleid ,req.params.tabname
    ).then(
        (data) => {
            console.log('In getFilterBusiness Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/architect/filterindustryv5/:userroleid/:tabname', function (req, res) {
    console.log('Input:' + req.params.userroleid);
	console.log('Input:' + req.params.tabname);
    commonService.getFilterIndustry(
        req.params.userroleid, req.params.tabname
    ).then(
        (data) => {
            console.log('In getFilterIndustry Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/design/businessprocessv5/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getArchitectBusinessProcess(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getArchitectBusinessProcess Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});


app.get('/design/kbdv5/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getArchitectKbd(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getArchitectKbd Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});


app.get('/design/interfacesv5/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getDesignInterface(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getDesignInterface Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});


app.get('/design/reportsv5/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getDesignReports(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getDesignReports Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/definedigitalorg/misc/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getDefineDigitalOrg(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getDefineDigitalOrg Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/refineuserstories/userstorylibrary/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getRefineUserstories(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getRefineUserstories Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/activatedigitalorg/misc/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getActivateDigitalOrg(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getActivateDigitalOrg Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/deploy/misc/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getDeploy(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getDeploy Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/design/ownthegapv5/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getDesignBusinessSolutions(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getDesignBusinessSolutions Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/construct/toolsv5/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getDesignDevTools(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getDesignDevTools Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/design/configurations/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getConstructERPConfigurations(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getConstructERPConfigurations Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/construct/conversion/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getConstructConversions(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getConstructConversions Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/validate/test/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getValidateTestScenarios(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getValidateTestScenarios Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/validate/bots/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getValidateTestBots(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getValidateTestBots Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/toolbar', function (req, res) {
    console.log('In toolbar');
    commonService.gettoolbar().then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/searchlinks', function (req, res) {
    console.log('In searchlinks');
    commonService.getsearchlinks().then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/design/kbdv5/:projectId', function (req, res) {
    console.log('In kbdv5::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postArchitectKbd(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/design/businessprocessv5/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postArchitectBusinessProcess(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/design/ownthegapv5/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postDesignBusinessSolutions(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/construct/toolsv5/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postDesignDevTools(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/design/reportsv5/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postDesignReports(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/design/interfacesv5/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postDesignInterface(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/design/configurations/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postConstructERPConfigurations(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/construct/conversion/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postConstructConversions(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/validate/bots/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postValidateTestBots(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/validate/test/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postValidateTestScenarios(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/design/userstoriesv5/:userroleid/:industry/:sector/:region/:l1/:l2/:l3',function(req,res){
    console.log('Input:'+req.params.userroleid);
    console.log('Input:'+req.params.industry);
    console.log('Input:'+req.params.sector);
    console.log('Input:'+req.params.region);
    console.log('Input:'+req.params.l1);
    console.log('Input:'+req.params.l2);
    console.log('Input:'+req.params.l3);

     imagineService.getUserStories(
     req.params.userroleid,
     req.params.industry,
     req.params.sector,
     req.params.region,
     req.params.l1,
     req.params.l2,
     req.params.l3
     ).then(
         (data)=>{
         console.log('In getUserStories Code:');
         console.log(data);
         res.send(data);
         }
     )
     .catch(function (err) {
         console.log(err);
         conn.close();
         return null;
       });
 });

 app.get('/architect/journeymap/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getJourneymap(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getJourneymap Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/architect/personas/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getPersonas(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getPersonas Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/phasestopinfo/:phasename/:stopname', function (req, res) {

    console.log('Input:' + req.params.userroleid);
	console.log('Input:' + req.params.phasename);
	console.log('Input:' + req.params.stopname);

	imagineService.getPhasestopinfo(
        req.params.userroleid, req.params.phasename, req.params.stopname
    ).then(
        (data) => {
            console.log('In getPhasestopinfo Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/stablize/misc/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    runservice.getstablizemisc(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getstablizemisc Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/optimize/regressiontest/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    runservice.getregressiontest(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getregressiontest Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/optimize/quarterlyinsights/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    runservice.getquarterlyinsights(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getquarterlyinsights Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/refineuserstories/config/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getrefineuserstoriesconfig(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getrefineuserstoriesconfig Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/refineuserstories/deliverables/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getrefineuserstoriesdeliverables(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getrefineuserstoriesdeliverables Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/allTabs/:phasename/:stopname', function (req, res) {
   // console.log('In Test::::' + req.body);
    //console.log(req.body);
	console.log('Input Phasename : ' +  req.params.phasename);
	console.log('Input Phasename : ' +  req.params.stopname);
    commonService.getAlltabsPhaseStop(req.params.phasename,req.params.stopname).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/optimize/quarterlyinsights/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    runservice.postquarterlyinsights(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/optimize/regressiontest/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    runservice.postregressiontest(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/stablize/misc/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    runservice.poststablizemisc(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/refineuserstories/config/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postrefineuserstoriesconfig(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/refineuserstories/deliverables/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postrefineuserstoriesdeliverables(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/definedigitalorg/misc/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postdefinedigitalorg(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/refineuserstories/userstorylibrary/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postrefineuserstories(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/activatedigitalorg/misc/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postactivatedigitalorg(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/deploy/misc/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postdeploy(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/design/userstoriesv5/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postUserStories(req.body, projectId).then(
        (data) => {
            console.log('In userstoriesv5 Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/architect/journeymap/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postJourneyMap(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/architect/personas/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postPersonas(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/toolinfo/:toolname',function(req,res){
    console.log('Input:'+req.params.toolname);


     commonService.gettoolsinfo(
     req.params.toolname
     ).then(
         (data)=>{
         console.log('In getUserStories Code:');
         console.log(data);
         res.send(data);
         }
     )
     .catch(function (err) {
         console.log(err);
         conn.close();
         return null;
       });
 });

//  app.get('/refineuserstories/userstorylibrary/:userroleid/:industry/:sector/:region/:l1/:l2/:l3',function(req,res){
//     console.log('Input:'+req.params.userroleid);
//     console.log('Input:'+req.params.industry);
//     console.log('Input:'+req.params.sector);
//     console.log('Input:'+req.params.region);
//     console.log('Input:'+req.params.l1);
//     console.log('Input:'+req.params.l2);
//     console.log('Input:'+req.params.l3);

//      imagineService.getUserStoryLibrary(
//      req.params.userroleid,
//      req.params.industry,
//      req.params.sector,
//      req.params.region,
//      req.params.l1,
//      req.params.l2,
//      req.params.l3
//      ).then(
//          (data)=>{
//          console.log('In getUserStoryLibrary Code:');
//          console.log(data);
//          res.send(data);
//          }
//      )
//      .catch(function (err) {
//          console.log(err);
//          conn.close();
//          return null;
//        });
//  });

//  app.post('/refineuserstories/userstorylibrary/:projectId', function (req, res) {
//     console.log('In userstorylibrary::::' + req.body);
//     console.log(req.body);
//     let projectId = req.params.projectId;
//     imagineService.postUserStoryLibrary(req.body, projectId).then(
//         (data) => {
//             console.log('In userstorylibrary post Code:');
//             console.log(data);
//             res.send(data);
//         }
//     )
//         .catch(function (err) {
//             console.log(err);
//             conn.close();
//             return null;
//         });
// });

app.post('/publish/email/:projectId', async (req, res) => {
    //console.log('Himanshu:'+req.params.projectId);

    let msgOutput;
    let projStatus;
    api_helper.getDBTeamStatus(req.params.projectId)
        .then(response1 => {
            if (response1 === 'NOT_INITIATED' || response1 === null) {
                console.log('Himanshu1:'+response1);
                msgOutput = 'Team creation process has been initiated.';
                const apiToken =  '{\"MSG\":\"' + msgOutput + '\"}';
                res.send(apiToken);
            }
            else {
                msgOutput = 'Team creation process has already been initiated.';
                console.log('Himanshu2:'+response1);
                const apiToken =  '{\"MSG\":\"' + msgOutput + '\"}';
                res.send(apiToken);
            };

	//let response1 = 'NOT_INITIATED';
    //  const apiToken = await '{\"MSG\":\"'+msgOutput+'\"}';
    //  res.send(apiToken);
    if (response1 === 'NOT_INITIATED' || response1 === null) {
        let errorArr = [];
        let idToken = req.headers.authorization;
        console.log('idToken: ' + idToken);
        idToken = idToken.replace('Bearer ', '');
        console.log('idToken: ' + idToken);
        api_helper.getGraphToken(idToken, connection.getconnection().createTeamsClientId, connection.getconnection().createTeamsClientSecret)
            .then(response => {

                console.log('response: ' + response);
                if (response) {
                    projectService.getProjectDetailsProject(req.params.projectId).then(
                        (data) => {
                            //console.log('In projectdetails Code:');
                            console.log('getProjectDetailsProject:' + data);
                            if (data)
                                var result = JSON.parse(data);
                            var projectName = result[0].projectName;
                            var projectManager = result[0].projectManager;
                            var givenName = result[0].givenName;
                            console.log('Project manager :' + projectManager);
                            console.log(givenName);


                            api_helper.getjoinedTeams(response, projectManager).then(
                                (data) => {
                                    //console.log('In Main Code:');
                                    console.log(data);
                                    //var userID = 'e85a5b31-bbd9-49d8-9868-ec2a715acc16'; //data.id;
                                    var userID = data.id;
                                    //res.send(userID);
                                    console.log('userID:' + userID);
                                    api_helper.createGroup(response, projectName, projectManager, userID).then(
                                        (data) => {

                                            if (data) {
                                                console.log('In createGroup:');
                                                console.log(data.id);
                                                var groupID = data.id;
                                                console.log("Hello Mansi2");

                                                //setTimeout(() => {}, 60000);
                                                    //   console.log("In Sleep1");
                                                    //   await api_helper.sleep(60000);
                                                    //   console.log("In Sleep2");



                                                //var now = new Date().getTime();
                                                //while (new Date().getTime() < now + 60000) {
                                                    //console.log('Hello');
                                                //}
                                                api_helper.createTeam(response, groupID).then(
                                                    (data) => {
                                                        if (data) {
															console.log('In createTeam:'+data);
															var result = JSON.parse(data.body);
															var linkURL = result.webUrl;
															//var result = JSON.parse(data);
															//let data1= JSON.stringify(data);
															//console.log('In createTeam1:'+data1);
															//console.log('In createTeam2:'+data1[0].statusCode);

															console.log('linkURL:'+linkURL);
															console.log('In createTeam3:'+data.statusCode);
															if (data.statusCode=== 200 || data.statusCode=== 201 ||data.statusCode=== 202) {
																console.log('In createTeam12:'+data.statusCode);
															}
															else
															{
																console.log('In createTeam:'+data.statusCode);
																throw "Error while creating team";
															}
                                                            //console.log(data);
                                                            //var resTeamDetails = JSON.parse(data);
                                                            //res.send(data);
															//let linkURL;
															//console.log('GetDriveDetailsforGroup groupID:'+groupID);
															//api_helper.GetDriveDetailsforGroup(response, groupID).then(
                                                            //                               (data) => {
                                                                                                //console.log('In GetDriveDetailsforGroup First DESTINATION:');
                                                                                                //linkURL = data.value[0].webUrl;
																								//console.log('linkURL:'+linkURL);
																								console.log('Sending mail');
																								api_helper.postemailserviceGraph(req.params.projectId,linkURL).then(
																								(data) => {
																									console.log('In postemailserviceGraph Code:');
																									console.log(data);
																									//res.send(data);
																								})
																								.catch(function (err) {
																									console.log(err);
																									errorArr.push(err);
																									console.log("Post mail Error Array:" + errorArr);
																									conn.close();
																									return null;
																								});
																						//	})



                                                            userService.getProjectMembersProject(req.params.projectId).then(
                                                                (data) => {
                                                                    console.log('In Main Code:');
                                                                    var resultMembersProject = JSON.parse(data);
                                                                    resultMembersProject = resultMembersProject[0].members;

                                                                    console.log("Members list " + resultMembersProject.length);

                                                                    for (var i = 0; i < resultMembersProject.length; i++) {
                                                                        console.log("Project Memebers list: ");
                                                                        api_helper.getjoinedTeams(response, resultMembersProject[i].userId).then(
                                                                            (data) => {
                                                                                console.log('In getjoinedTeams Members:');
                                                                                console.log(data.id);
                                                                                if (userID === data.id) {
                                                                                    console.log(data.id + ' is already an owner');
                                                                                }
                                                                                else {
                                                                                    console.log(data.id + ' adding');
                                                                                    api_helper.AddMemberToTeam(response, groupID, data.id);
                                                                                }
                                                                                //api_helper.AddMemberToTeam(response, groupID, data.id)
                                                                                //var resTeamDetails = JSON.parse(data);
                                                                                //res.send(data);
                                                                            }
                                                                        )
                                                                            .catch(function (err) {
                                                                                console.log(err);
                                                                                errorArr.push(err);
                                                                                console.log("GetJoinedTeams Error Array" + errorArr);
                                                                                //conn.close();
                                                                                //return null;
                                                                            })

                                                                        console.log("MemberName: ");
                                                                        console.log(resultMembersProject[i].userId);
                                                                    }
                                                                    //res.send(resultMembersProject[0].userId);
                                                                }
                                                            )
                                                                .catch(function (err) {
                                                                    console.log(err);
                                                                    errorArr.push(err);
                                                                    console.log("getProjectMembersProject Error Array" + errorArr);
                                                                    //conn.close();
                                                                    //return null;
                                                                });

                                                        }
                                                        else {
                                                            errorArr.push('No response received after teams creation');
                                                            console.log('Send error mail:' + errorArr.length);
                                                            api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                                                                (data) => {
                                                                    console.log('In Error Mail:');
                                                                    console.log(errorArr);
                                                                });
                                                        }

                                                        api_helper.getDummyStructure(response).then(
                                                            (data) => {

                                                                console.log('In getDummyStructure New:');
                                                                if (data) {
                                                                    console.log(data);
                                                                    var resSrcGroupID = (data.value[0].id);
                                                                    //resSrcGroupID = resSrcGroupID;
                                                                    console.log('resSrcGroupID: ' + resSrcGroupID);
                                                                    api_helper.GetDriveDetailsforGroup(response, resSrcGroupID).then(
                                                                        (data) => {
                                                                            console.log(data);
																			console.log('In GetDriveDetailsforGroup Source Code:');
                                                                            console.log(data.value[0].id);
                                                                            var resDriveID = data.value[0].id;
                                                                            //res.send(data);
                                                                            api_helper.GetSourceFolderDetails(response, resDriveID).then(
                                                                                (data) => {
                                                                                    console.log('In GetSourceFolderDetails Code:');
                                                                                    console.log(data.id);
                                                                                    if (data) {
                                                                                        var fldrStrtSrcId = data.id;
                                                                                        //res.send(data);
                                                                                        api_helper.GetDriveDetailsforGroup(response, groupID).then(
                                                                                            (data) => {
                                                                                                console.log('In GetDriveDetailsforGroup DESTINATION:');
                                                                                                var driveID = data.value[0].id;
                                                                                                console.log('GetDriveDetailsforGroup DESTINATION: ' + driveID);
                                                                                                //res.send(driveID);
                                                                                                // var now = new Date().getTime();
                                                                                                // while (new Date().getTime() < now + 20000) {
                                                                                                //     //console.log('Hello');
                                                                                                // }
                                                                                                api_helper.GettingFileDetailswithinTestTeamFolder(response, driveID).then(
                                                                                                    (data) => {
                                                                                                        //console.log('In createTeam:');
                                                                                                        console.log(data);
                                                                                                        console.log('GetFolderDetailsforGroup DESTINATION: ' + data);
                                                                                                        var folderID = data.id;
                                                                                                        //res.send(folderID);


                                                                                                        api_helper.copyFolderStruct(response, resSrcGroupID, fldrStrtSrcId, driveID, folderID).then(
                                                                                                            (data) => {
                                                                                                                console.log('In Main Code copyFolderStruct:');
                                                                                                                console.log(data);
                                                                                                                //var resTeamDetails = JSON.parse(data);
                                                                                                                //res.send(data);
                                                                                                                // while (new Date().getTime() < now + 120000) {
                                                                                                                //     //console.log('Hello');
                                                                                                                // }
                                                                                                                api_helper.GetSourceFileDetails(response, resSrcGroupID, driveID, groupID, req.params.projectId).then(
                                                                                                                    (data) => {
                                                                                                                        console.log('In GetSourceFileDetails:');
                                                                                                                        console.log(data);
                                                                                                                        console.log('data.length:' + data.length);
                                                                                                                        if (data.length > 0) {
                                                                                                                            console.log('Errored files: ' + data);
                                                                                                                            errorArr.push(data);
                                                                                                                            api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                                                                                                                                (data) => {
                                                                                                                                    console.log('In Error Mail files:');
                                                                                                                                    console.log(errorArr);
                                                                                                                                    //conn.close();
                                                                                                                                    return null;
                                                                                                                                })
                                                                                                                        }
                                                                                                                        //var resTeamDetails = JSON.parse(data);
                                                                                                                        //res.send(data);
                                                                                                                    })
                                                                                                                    .catch(function (err) {
                                                                                                                        console.log(err);
                                                                                                                        errorArr.push(err);
                                                                                                                        console.log("GetSourceFileDetails Error Array:" + errorArr);
                                                                                                                        api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                                                                                                                                (data) => {
                                                                                                                                    console.log('In Error Mail:');
                                                                                                                                    console.log(errorArr);
                                                                                                                                    //conn.close();
                                                                                                                                    return null;
                                                                                                                                })
                                                                                                                        //conn.close();
                                                                                                                        return null;
                                                                                                                    })
                                                                                                            })
                                                                                                            .catch(function (err) {
                                                                                                                console.log(err);
                                                                                                                errorArr.push(err);
                                                                                                                console.log("copyFolderStruct Error Array:" + errorArr);
                                                                                                                //conn.close();
                                                                                                                return null;
                                                                                                            })
                                                                                                    })
                                                                                                    .catch(function (err) {
                                                                                                        console.log(err);
                                                                                                        errorArr.push(err);
                                                                                                        console.log("GettingFileDetailswithinTestTeamFolder Error Array:" + errorArr);

                                                                                                        //conn.close();
                                                                                                        return null;
                                                                                                    })
                                                                                            })
                                                                                            .catch(function (err) {
                                                                                                console.log(err);
                                                                                                errorArr.push(err);
                                                                                                console.log("GetDriveDetailsforGroup Error Array:" + errorArr);
                                                                                                api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                                                                                                    (data) => {
                                                                                                        console.log('In Error Mail:');
                                                                                                        console.log(errorArr);
                                                                                                        //conn.close();
                                                                                                        return null;
                                                                                                    })

                                                                                            })
                                                                                    }
                                                                                    else {
                                                                                        errorArr.push('No response received after folder creation');
                                                                                        console.log('Send error mail:' + errorArr.length);
                                                                                        api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                                                                                            (data) => {
                                                                                                console.log('In Error Mail:');
                                                                                                console.log(errorArr);
                                                                                            })
                                                                                    }
                                                                                })
                                                                                .catch(function (err) {
                                                                                    console.log(err);
                                                                                    errorArr.push(err);
                                                                                    console.log("GetSourceFolderDetails Error Arrray:" + errorArr);
                                                                                    //conn.close();
                                                                                    return null;
                                                                                });

                                                                        }
                                                                    )
                                                                        .catch(function (err) {
                                                                            console.log(err);
                                                                            errorArr.push(err);
                                                                            console.log("GetDriveDetailsforGroup Error Array:" + errorArr);
                                                                            //conn.close();
                                                                            return null;
                                                                        })
                                                                }
                                                                else {
                                                                    errorArr.push('No response received after dummy structure creation');
                                                                    console.log('Send error mail:' + errorArr.length);
                                                                    api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                                                                        (data) => {
                                                                            console.log('In Error Mail:');
                                                                            console.log(errorArr);
                                                                            //res.send(data);
                                                                        }
                                                                    )
                                                                }
                                                            }
                                                        )
                                                            .catch(function (err) {
                                                                console.log(err);
                                                                errorArr.push(err);
                                                                console.log("Get dummy structure Error Array:" + errorArr);
                                                                //conn.close();
                                                                //return null;
                                                            })


                                                    }).catch(function (err) {
                                                                console.log(err);
                                                                errorArr.push(err);
                                                                console.log("createTeam Error Array:" + errorArr);
																api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
																(data) => {
																	console.log('In Error Mail:');
																	console.log(errorArr);
																	//res.send(data);
																})
                                                                //conn.close();
                                                                //return null;
                                                            })
                                            }

                                            else {
                                                errorArr.push('No response received for group creation');
                                                console.log('Send error mail:' + errorArr.length);
                                                api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                                                    (data) => {
                                                        console.log('In Error Mail:');
                                                        console.log(errorArr);
                                                        //res.send(data);
                                                    })
                                            }
                                        })
                                        .catch(function (err) {
                                            console.log(err);
                                            errorArr.push("Create Group Error Array" + errorArr);
                                            console.log("Create Group Error Array" + errorArr);
                                            api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                                                (data) => {
                                                    console.log('In Error Mail:');
                                                    console.log(errorArr);
                                                    //res.send(data);
                                                }
                                            )
                                            //conn.close();
                                            return null;
                                        })


                                })
                                .catch(function (err) {
                                    console.log(err);
                                    errorArr.push(err);
                                    console.log("Joined Teams Error Array" + errorArr);
									api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                        (data) => {
                            console.log('In Error Mail:');
                            console.log(errorArr);
                            //res.send(data);
							}
							)
                                    //conn.close();
                                    return null;
                                });



                            //res.send(result[0].projectName);

                            // if (!data)
                            //     res.send('null')
                        }
                    )
                        .catch(function (err) {
                            console.log(err);
                            errorArr.push(err);
                            console.log("Outer Catch Error Array: " + errorArr);
							api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
							(data) => {
								console.log('In Error Mail:');
								console.log(errorArr);
								//res.send(data);
									}
							)
                            //conn.close();
                            return null;
                        });


                }

                else {
                    //send error mail
                    errorArr.push('No response received while fetching access token');
                    console.log('send error mail:' + errorArr.length);
                    api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                        (data) => {
                            console.log('In Error Mail:');
                            console.log(errorArr);
                            //res.send(data);
                        }
                    )
                }
            }
            )


            .catch(error => {
                res.send(error);
                errorArr.push(error);
                var now1 = new Date().getTime();
                while (new Date().getTime() < now1 + 300000) {
                    //console.log('Hello');
                }
                console.log('Out Error catch Code:' + errorArr.length);
                if (errorArr.length > 0) {
                    console.log('In Error catch Code:' + errorArr.length);
                    api_helper.postemailerrorserviceGraph(req.params.projectId, errorArr).then(
                        (data) => {
                            console.log('In Error Code:');
                            console.log('Final Error Array' + errorArr);
                            //res.send(data);
                        }
                    )
                }
            })

    }

});
    //res.send('Hi')
});

app.get('/launchjourney/ocm/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getLaunchJourneyOCM(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getLaunchJourneyOCM Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/launchjourney/ocm/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postLaunchJourneyOCM(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/refineuserstories/ocm/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getRefineUserStoriesOCM(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getRefineUserStoriesOCM Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/refineuserstories/ocm/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postRefineUserStoriesOCM(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/activatedigitalorganization/ocm/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getActivateDigitalOrganizationOCM(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getActivateDigitalOrganizationOCM Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/activatedigitalorganization/ocm/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postActivateDigitalOrganizationOCM(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/construct/ocm/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getConstructOCM(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getConstructOCM Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/construct/ocm/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postConstructOCM(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/validate/ocm/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getValidateOCM(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getValidateOCM Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/validate/ocm/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postValidateOCM(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});


app.get('/deploy/ocm/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getDeployOCM(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getDeployOCM Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.post('/deploy/ocm/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postDeployOCM(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/continuedigitalorg/misc/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    runservice.getContinueDigitalOrg(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getContinueDigitalOrg Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});


app.post('/continuedigitalorg/misc/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    runservice.postcontinuedigitalorg(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});


app.get('/launchjourney/misc/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    imagineService.getLaunchJourney(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getLaunchJourney Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});


app.post('/launchjourney/misc/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    imagineService.postLaunchJourney(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/validatedeliverables/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getvalidatedeliverables(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getvalidatedeliverables Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});


app.post('/validatedeliverables/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postvalidatedeliverables(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/constructdeliverables/:userroleid/:industry/:sector/:region/:l1/:l2/:l3', function (req, res) {
    console.log('Input:' + req.params.userroleid);
    console.log('Input:' + req.params.industry);
    console.log('Input:' + req.params.sector);
    console.log('Input:' + req.params.region);
    console.log('Input:' + req.params.l1);
    console.log('Input:' + req.params.l2);
    console.log('Input:' + req.params.l3);

    deliverService.getconstructdeliverables(
        req.params.userroleid,
        req.params.industry,
        req.params.sector,
        req.params.region,
        req.params.l1,
        req.params.l2,
        req.params.l3
    ).then(
        (data) => {
            console.log('In getconstructdeliverables Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});


app.post('/constructdeliverables/:projectId', function (req, res) {
    console.log('In Test::::' + req.body);
    console.log(req.body);
    let projectId = req.params.projectId;
    deliverService.postconstructdeliverables(req.body, projectId).then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});
app.get('/tutorials', function (req, res) {
    console.log('In Test::::' + req.body);
   // console.log(req.body);
    //let projectId = req.params.projectId;
    commonService.gettutorials().then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/marketingmaterials', function (req, res) {
    console.log('In Test::::' + req.body);
   // console.log(req.body);
    //let projectId = req.params.projectId;
    commonService.getmarketingmaterials().then(
        (data) => {
            console.log('In Main Code:');
            console.log(data);
            res.send(data);
        }
    )
        .catch(function (err) {
            console.log(err);
            conn.close();
            return null;
        });
});

app.get('/graphAuth123/:projectID',function(req,res){
    console.log('Input:'+req.params.toolname);


     api_helper.UpdateDBTeamStatus(
     req.params.projectid
     ).then(
         (data)=>{
         console.log('In getUserStories Code:');

         res.send('Done');
         }
     )
     .catch(function (err) {
         console.log(err);
         conn.close();
         return null;
       });
 });

 function printHello(){
     console.log('Hello to all')
 }

 app.get('/testservice', function (req, res) {
    console.log('In projectDetailsPage::::');
            //console.log(data);
            let data = {resData:"dadaerte"};
            printHello();
            res.send(data);
     
});

var server = app.listen(process.env.PORT , function () {

    console.log("Website Host" + ':' + process.env.WEBSITE_HOSTNAME );

    var host = server.address().address
    var port = server.address().port

    console.log("DB Service app listening at http://%s:%s", host, port);

    setInterval(() => {console.log('Testing')}, 2000);

})
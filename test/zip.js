var Zip = require('adm-zip');

var zip = new Zip("D:\\down1\\n\\[图包]美丝学校-伊甸园人体写真第二期.zip");

var zipEntries = zip.getEntries();
zipEntries.forEach(function(e) {
    console.log(e.name, e.entryName, e.isDirectory);
});
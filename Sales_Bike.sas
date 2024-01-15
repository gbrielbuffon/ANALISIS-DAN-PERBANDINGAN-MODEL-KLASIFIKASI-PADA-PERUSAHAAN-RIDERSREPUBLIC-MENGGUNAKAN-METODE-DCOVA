session server;

/* Start checking for existence of each input table */
exists0=doesTableExist("CASUSER(gabriel.buffon@student.umn.ac.id)", "SALES");
if exists0 == 0 then do;
  print "Table "||"CASUSER(gabriel.buffon@student.umn.ac.id)"||"."||"SALES" || " does not exist.";
  print "UserErrorCode: 100";
  exit 1;
end;
print "Input table: "||"CASUSER(gabriel.buffon@student.umn.ac.id)"||"."||"SALES"||" found.";
/* End checking for existence of each input table */


  _dp_inputTable="SALES";
  _dp_inputCaslib="CASUSER(gabriel.buffon@student.umn.ac.id)";

  _dp_outputTable="0cb4ee0b-5c60-4947-9d36-83624f5c6946";
  _dp_outputCaslib="CASUSER(gabriel.buffon@student.umn.ac.id)";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "0cb4ee0b-5c60-4947-9d36-83624f5c6946" (caslib="CASUSER(gabriel.buffon@student.umn.ac.id)" promote="no");

    length
       "Hari"n 8
    ;
    label
        "Hari"n=""
    ;
    format
        "Hari"n 12.
    ;

    /* Set the input                                                                set */
    set "SALES" (caslib="CASUSER(gabriel.buffon@student.umn.ac.id)"   drop="Date"n  rename=("Day"n = "Hari"n) );

    /* BEGIN statement 4093b46f_3c42_417d_bbf0_dae694d89ca3                      casing */
    "Month"n = kupcase("Month"n);
    /* END statement 4093b46f_3c42_417d_bbf0_dae694d89ca3                        casing */

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="0cb4ee0b-5c60-4947-9d36-83624f5c6946";
  _dp_inputCaslib="CASUSER(gabriel.buffon@student.umn.ac.id)";

  _dp_outputTable="SALES_NEW";
  _dp_outputCaslib="CASUSER(gabriel.buffon@student.umn.ac.id)";

srcCasTable="0cb4ee0b-5c60-4947-9d36-83624f5c6946";
srcCasLib="CASUSER(gabriel.buffon@student.umn.ac.id)";
tgtCasTable="SALES_NEW";
tgtCasLib="CASUSER(gabriel.buffon@student.umn.ac.id)";
saveType="sashdat";
tgtCasTableLabel="";
replace=1;
saveToDisk=1;

exists = doesTableExist(tgtCasLib, tgtCasTable);
if (exists !=0) then do;
  if (replace == 0) then do;
    print "Table already exists and replace flag is set to false.";
    exit ({severity=2, reason=5, formatted="Table already exists and replace flag is set to false.", statusCode=9});
  end;
end;

if (saveToDisk == 1) then do;
  /* Save will automatically save as type represented by file ext */
  saveName=tgtCasTable;
  if(saveType != "") then do;
    saveName=tgtCasTable || "." || saveType;
  end;
  table.save result=r status=rc / caslib=tgtCasLib name=saveName replace=replace
    table={
      caslib=srcCasLib
      name=srcCasTable
    };
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
  tgtCasPath=dictionary(r, "name");

  dropTableIfExists(tgtCasLib, tgtCasTable);
  dropTableIfExists(tgtCasLib, tgtCasTable);

  table.loadtable result=r status=rc / caslib=tgtCasLib path=tgtCasPath casout={name=tgtCasTable caslib=tgtCasLib} promote=1;
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
end;

else do;
  dropTableIfExists(tgtCasLib, tgtCasTable);
  dropTableIfExists(tgtCasLib, tgtCasTable);
  table.promote result=r status=rc / caslib=srcCasLib name=srcCasTable target=tgtCasTable targetLib=tgtCasLib;
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
end;


dropTableIfExists("CASUSER(gabriel.buffon@student.umn.ac.id)", "0cb4ee0b-5c60-4947-9d36-83624f5c6946");

function doesTableExist(casLib, casTable);
  table.tableExists result=r status=rc / caslib=casLib table=casTable;
  tableExists = dictionary(r, "exists");
  return tableExists;
end func;

function dropTableIfExists(casLib,casTable);
  tableExists = doesTableExist(casLib, casTable);
  if tableExists != 0 then do;
    print "Dropping table: "||casLib||"."||casTable;
    table.dropTable result=r status=rc/ caslib=casLib table=casTable quiet=0;
    if rc.statusCode != 0 then do;
      exit();
    end;
  end;
end func;

/* Return list of columns in a table */
function columnList(casLib, casTable);
  table.columnInfo result=collist / table={caslib=casLib,name=casTable};
  ndimen=dim(collist['columninfo']);
  featurelist={};
  do i =  1 to ndimen;
    featurelist[i]=upcase(collist['columninfo'][i][1]);
  end;
  return featurelist;
end func;

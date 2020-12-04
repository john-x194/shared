var version = "";
function pbjsVersion() 
{
    try
    {
        version = pbjs.version;
        


    }
    catch(err)
    {
        
    }
}

pbjsVersion();
return version;
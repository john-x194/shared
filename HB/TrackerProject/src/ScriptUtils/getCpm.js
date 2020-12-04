function getCPM()
{    
    var responses = pbjs.getBidResponses();
    var winners = pbjs.getAllWinningBids();
    var output = [];
    var pbOutput = [];

    Object.keys(responses).forEach(function(adUnitCode) {
    var response = responses[adUnitCode];

        response.bids.forEach(function(bid) 
        {
            output.push({
            bid: bid,
            adunit: adUnitCode,
            adId: bid.adId,
            bidder: bid.bidder,
            time: bid.timeToRespond,
            cpm: bid.cpm,
            msg: bid.statusMessage,
            rendered: !!winners.find(function(winner) {
                return winner.adId==bid.adId;
            })
            
            });
        });
    });

    if (output.length) 
    {
        return output;
    }
    else 
    {
    console.warn('NO prebid responses');
    }
}
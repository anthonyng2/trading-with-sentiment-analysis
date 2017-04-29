from quantopian.pipeline import Pipeline
from quantopian.algorithm import attach_pipeline, pipeline_output

from quantopian.pipeline.filters.morningstar import Q1500US
from quantopian.pipeline.data.psychsignal import stocktwits

from quantopian.pipeline.factors.eventvestor import (
     BusinessDaysUntilNextEarnings,
     BusinessDaysSincePreviousEarnings,
)

from quantopian.pipeline.filters.eventvestor import IsAnnouncedAcqTarget

from quantopian.pipeline.factors import BusinessDaysSincePreviousEvent



def initialize(context):
    """
    Called once at the start of the algorithm.
    """   
    # Rebalance every day, 1 hour after market open.
    schedule_function(my_rebalance, date_rules.every_day(), time_rules.market_open(hours=1))
     
    # Record tracking variables at the end of each day.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())
     
    # Create our dynamic stock selector.
    attach_pipeline(make_pipeline(), 'my_pipeline')
    
    set_commission(commission.PerTrade(cost=0.000))

    
    
    
def make_pipeline():

    sentiment_factor = stocktwits.bull_minus_bear.latest

    not_near_earnings_announcement = ~((BusinessDaysUntilNextEarnings() <= 2)
                                    |(BusinessDaysSincePreviousEarnings() <= 2))
    
    new_info = (BusinessDaysSincePreviousEvent(inputs=[stocktwits.asof_date.latest]) <= 1)
    
    not_accounced_acq_target = ~IsAnnouncedAcqTarget()
    
    universe = (Q1500US()
               & sentiment_factor.notnull()
               & not_near_earnings_announcement
               & not_accounced_acq_target
               & new_info)    
    

    # A classifier to separate the stocks into quantiles based on sentiment rank.
    sentiment_quantiles = sentiment_factor.rank(mask=universe, method='average').quantiles(2)
    
    # 
    pipe = Pipeline(
        columns={
            'sentiment': sentiment_quantiles,
            'shorts': sentiment_factor <= -2,
            'longs': sentiment_factor >= 2.5,
        },
        screen=universe
    )
    
    return pipe




def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    context.output = pipeline_output('my_pipeline')
  
    # These are the securities that we are interested in trading each day.
    context.security_list = context.output.index.tolist()
    

    
    
def my_rebalance(context,data):
    """
    Place orders according to our schedule_function() timing.
    """
    
    # Compute our portfolio weights.
    long_secs = context.output[context.output['longs']].index
    try:
        long_weight = 1.0 / len(long_secs)
    except:
        long_weight = 0
    
    short_secs = context.output[context.output['shorts']].index
    try:
        short_weight = -1.0 / len(short_secs)
    except:
        short_weight = 0

    # Open our long positions.
    for security in long_secs:
        if data.can_trade(security):
            order_target_percent(security, long_weight)
    
    # Open our short positions.
    for security in short_secs:
        if data.can_trade(security):
            order_target_percent(security, short_weight)

    # Close positions that are no longer in our pipeline.
    for security in context.portfolio.positions:
        if data.can_trade(security) and security not in long_secs and security not in short_secs:
            order_target_percent(security, 0)

            
            
            
 
def my_record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    long_count = 0
    short_count = 0

    for position in context.portfolio.positions.itervalues():
        if position.amount > 0:
            long_count += 1
        if position.amount < 0:
            short_count += 1
            
    # Plot the counts
    record(num_long=long_count, num_short=short_count, leverage=context.account.leverage)

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

def plotvis(result_df, schedule_interval, category_cols=None):
    if schedule_interval > 5:
        line_plot(result_df, category_cols)
    else:
        barplot(result_df, schedule_interval, category_cols)
        
def line_plot(result, category_cols=None):
    if category_cols == None:
        figsize=(10, 5)
        fig, axs = plt.subplots(1, 1, constrained_layout=False, figsize=figsize)
     
        temp = result.copy()
        axs.plot(temp['date'], temp['forecast'], c='#0072B2')
        axs.set_title('Forecasting Result')
        axs.xaxis.set_major_locator(mdates.DayLocator())
        axs.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        axs.xaxis.set_tick_params(rotation=90)
        fig.savefig('forecast_result.pdf')
    else:
        plt.style.use('ggplot')
 
        cat = result[category_cols].unique().tolist()
        figsize=(10, len(cat)*5)
        fig, axs = plt.subplots(len(cat), 1, constrained_layout=False, figsize=figsize)
        plt.subplots_adjust(hspace=0.6)
        
        for idx,i in enumerate(cat):
            temp = result[result[category_cols]==i]
            axs[idx].plot(temp['date'], temp['forecast'], c='#0072B2')
            axs[idx].set_title('Forecasting {}'.format(i))
            axs[idx].xaxis.set_major_locator(mdates.DayLocator())
            axs[idx].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
            axs[idx].xaxis.set_tick_params(rotation=90)

        fig.savefig('forecast_result.pdf')

def barplot(result_df,schedule_interval, category_cols=None):    
    if (category_cols== None):

        fig, ax = plt.subplots()
        plt.style.use('ggplot')
        agg = sum(result_df.forecast)
        ax.bar('Forecasting result for the next {} days'.format(schedule_interval),agg, align='center', width=0.3)

        plt.xticks(rotation=0)
        totals = []
        for i in ax.patches:
            totals.append(i.get_height())
        for i in ax.patches:
            # get_x pulls left or right; get_height pushes up or down
            ax.text(i.get_x() + i.get_width()/2-.05, i.get_height()/2, i.get_height(),
                        color='white', fontsize=30)
        ax.get_figure().savefig('forecast_result.pdf')
    
        
    else:
        plt.style.use('ggplot')
        aggregate =result_df.groupby(by=category_cols).agg({'forecast':sum}).reset_index().sort_values(by='forecast', ascending=False)
        ax =aggregate.plot(kind='bar', x=category_cols, y='forecast', color ="coral")

        ax.set_alpha(0.8)
        ax.set_xlabel('Date')
        ax.set_title("Forecasting Result")

        plt.xticks(rotation=90)

        # create a list to collect the plt.patches data
        totals = []

        # find the values and append to list
        for i in ax.patches:
            totals.append(i.get_height())

        # set individual bar lables using above list
        for i in ax.patches:
            # get_x pulls left or right; get_height pushes up or down
            ax.text(i.get_x()-.03, i.get_height()+.5, i.get_height(),
                        color='dimgrey', rotation=90)
        ax.get_figure().savefig('forecast_result.pdf')
    

        
        
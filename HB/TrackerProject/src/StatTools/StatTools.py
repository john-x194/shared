import os
import json
import argparse
import numpy as np
import pandas as pd 
from pylab import exp
import matplotlib as mpl
import scipy.stats as ss
from matplotlib import mlab
from tabulate import tabulate
import matplotlib.pyplot as plt
from  pprint import pprint as pp
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,
                               AutoMinorLocator)

class StatTools:
    def __init__(self, **kwargs):
        self.input_file=kwargs['input_file']
        menu_range = range(1,4)
        menu_print = {"1)":"Plot Histogram", 
                      "2)":"Plot BoxPlot",
                      "3)":"Plot CDF"}

        for x in menu_print: 
            print("{}{}".format(x, menu_print[x]))
        self.yes = ['Y', 'y', 'true', 'True', True,'Yes', 'yes']
        self.axis_labels = {"xlabel":"","ylabel":"","title":""}
        self.axis_scales = {"x_axis":{"x_major":[], "x_minor":[]}, "y_axis":{"y_major":[], "y_minor":[]}}
        self.type = input('Enter choice: ')
        while int(self.type) not in menu_range: 
            for x in menu_print: 
                print("{}{}".format(x, menu_print[x]))
            self.type = input('Enter choice from menu: ')
        if not os.path.isdir('plots'):
            os.mkdir('plots')
        self.dispatchFunction(self.type)


    def setPltAxisTicks(self, set_x_scale, set_y_scale):
        if set_x_scale in self.yes:
            self.axis_scales['x_axis']['x_major'] = input("Enter major x-axis tick interval: ")
        if set_y_scale in self.yes:
            self.axis_scales['y_axis']['y_major'] = input("Enter major y-axis tick interval: ")

    def setPlotAxisTitles(self):
        self.axis_labels['xlabel'] = input('Enter x-axis label: ')
        self.axis_labels['ylabel'] = input('Enter y-axis label: ')
        self.axis_labels['title'] = input('Enter plot title: ')


    def plotHistogram(self, population=None):
        bin_algs = ['auto', 'sturges', 'fd', 'doane', 'scott', 'rice', 'sturges' , 'sqrt']
        N = input("Enter number of bins or binning strategy\n (auto, sturges, fd, doane, scott, rice, sturges or sqrt): ")
        
        if N == '': 
            N = 'auto'
        if N not in bin_algs:
            N = int(N)
   
        print(N)

        fig, ax = plt.subplots(figsize=(8, 4))
        bins = N
        if type(N) == int:
            bins = list(range(1,N+2))

        print(bins)

        ax.hist(population,bins = bins, align='left')
        ax.set_title(self.axis_labels['title'])
        ax.set_xlabel(self.axis_labels['xlabel'])
        ax.set_ylabel(self.axis_labels['ylabel'])
        if type(bins) == list: 
            ax.set_xticks(bins, minor=False)
        plt.show()
    
    def plotBox(self, population=None):

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.boxplot(population)
        ax.set_title(self.axis_labels['title'])
        ax.set_xlabel(self.axis_labels['xlabel'])
        ax.set_ylabel(self.axis_labels['ylabel'])
        plt.show()

    def plotCDF(self, population=None):
        bin_algs = ['auto', 'sturges', 'fd', 'doane', 'scott', 'rice', 'sturges' , 'sqrt']
        N = input("Enter number of bins or binning strategy\n (auto, sturges, fd, doane, scott, rice, sturges or sqrt) :")
        if not N:    
            N = 'auto'
        if N not in bin_algs:
            N = int(N)   
        fig, ax = plt.subplots(figsize=(8, 4))

        ax.hist(population, bins=N, density=True, cumulative=True, histtype='step', color='purple', align='left')

        # ax.legend(loc='upper left')
        ax.set_title(self.axis_labels['title'])
        ax.set_xlabel(self.axis_labels['xlabel'])
        ax.set_ylabel(self.axis_labels['ylabel'])
        plt.show()


    def dispatchFunction(self, param):
        functionMap = {"1":self.plotHistogram, 
                       "2":self.plotBox,
                       "3":self.plotCDF
                      }
        population = self.getData(self.input_file)
        functionMap[param](population)

    def getData(self, filepath):
        data = pd.read_csv(filepath)  
        column_menu = {}
        column_lookup = {}
        x_data = -1
        for (col, index) in zip(data.columns, data.index): 
            column_menu['{})'.format(index)] = col
            column_lookup[index] = col
        for index in column_menu:
            print(index, column_menu[index])
        x_data = int(input('Enter number corresponding to col name from input file (x-axis): ')  )
        try: 
            x_data = data[column_lookup[x_data]].values
            print(x_data)
        except: 
            while(x_data not in column_lookup):
                for index in column_menu:
                    print(index, column_menu[index])
                x_data = input('Enter number corresponding to col name from table above: ')   
        self.setPlotAxisTitles()
    
        return x_data
  
            


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', help="This is the file which data will be used to generate stats and plots. Supported Format(s): CSV")
    args = parser.parse_args()
    file_path = os.path.abspath(args.input_file)
    print(file_path)
    dist = StatTools(input_file=file_path)


import pandas as pd
import numpy as np
from ImportUtils import vprint, vprint_time

import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.colors as mcolors
import locale

# Set the locale to your system's default (for the desired thousands separator)
locale.setlocale(locale.LC_ALL, 'pt_PT')

plt.style.use('fivethirtyeight')
# other interesting and well-knwon styles: 'ggplot', 'seaborn'

K_MATPLOTLIB_STYLES_FILE = "NOVAENES.mplstyle"


# define colormap NOVAIMS
#colors = ["#BED62F", "#5C666C"]

cdict = {
    'red': [(0, 0.5, 0.5), (1.0, 0.5, 0.5)],
    'green': [(0, 0.5, 0.5), (1.0, 0.8, 0.8)],
    'blue': [(0, 0.5, 0.5), (1.0, 0.2, 0.2)]
}

try:
	plt.cm.unregister_cmap('NOVAIMS')
except:
	pass

# create the colormap from the dictionary
nova_cmap = mcolors.LinearSegmentedColormap('nova_cmap', cdict)

# register the colormap with matplotlib
plt.cm.register_cmap('NOVAIMS', nova_cmap)


#region set_matplotlib_rcParams
def set_matplotlib_rcParams():
	# Set the default style
	plt.style.use('fivethirtyeight')
	# Other interesting and well-knwon styles: 'ggplot', 'seaborn'

	rcParamsFromFile = mpl.rc_params_from_file(K_MATPLOTLIB_STYLES_FILE, fail_on_error=True, use_default_template=False)
	
	mpl.rcParams.update(rcParamsFromFile)
#endregion set_matplotlib_rcParams
set_matplotlib_rcParams()


#region chart
def chart(kind="line", df=None, stacked=False, normalize=False, \
	xvar=None, xlabel = '', xlimit=None, xAxisScale=None, xAxisScaleSymbol=None, \
	yvar='count', ylabel='Number of exams', ylimit=(0, None), yAxisScale=None, yAxisScaleSymbol=None, \
	zvar="", zlabel = "", \
	title=None, grid=True, colormap='NOVAIMS', legend=True, xticks=None, yticks=None, figsize=(8,4.5), \
	dots = []):
	'''Minimum parameters: chart(kind="line", df=dataframe, xvar='Class_Exam')'''
	# Pivot the DataFrame to create the data for the stacked bar chart

	if zvar == '' or zvar == None:
		if yvar == None:
			df_grouped = df.groupby(xvar).size().reset_index(xvar)
		else:
			df_grouped = df.groupby(xvar)[yvar].sum().reset_index(xvar)

	else:
		if yvar == None:
			df_grouped = df.groupby(xvar)[zvar].value_counts(normalize=normalize).unstack(zvar)
			df_grouped = df_grouped.reset_index()
		else:
			df_grouped = df.groupby([xvar,zvar])[yvar].sum().unstack(zvar)
			df_grouped = df_grouped.reset_index(xvar)
		


	if kind in ['bar', 'barh']:
		# only difference is that we're setting the width of each bar to 0.8
		plt = df_grouped.plot(kind=kind, stacked=stacked, \
				x=xvar, xlabel=xlabel, xlim=xlimit, \
				ylim=ylimit, ylabel=ylabel, \
				colormap=colormap, figsize=figsize, title=title, rot=0, fontsize=12, legend=legend, width = 0.8,
				ax=None, subplots=False, xticks=xticks, yticks=yticks)
		# 		y=yvar,
			
	else:
		plt = df_grouped.plot(kind=kind, stacked=stacked, \
				x=xvar, xlabel=xlabel, xlim=xlimit, \
				ylim=ylimit, ylabel=ylabel, \
				colormap=colormap, figsize=figsize, title=title, rot=0, fontsize=12, legend=legend,
				ax=None, subplots=False, xticks=xticks, yticks=yticks)


	if legend:
		plt.legend(title = zlabel)

	if isinstance(grid, bool):
		if grid == True:
			plt.grid(True)
		else:
			plt.grid(False)
	elif isinstance(grid, str):
		grid = grid.lower()
		if 'x' in grid and 'y' in grid:
			plt.grid(True)
		elif 'x' in grid:
			plt.xaxis.grid(which='major')
		elif 'y' in grid:
			plt.yaxis.grid(which='major')
		else:
			plt.grid(False)
	
	if not xAxisScale is None or not xAxisScaleSymbol is None:
		if xAxisScale is None:
			xAxisScale = 1
		if xAxisScaleSymbol is None:
			xAxisScaleSymbol = ''
		plt.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{x*xAxisScale:,.0f}{xAxisScaleSymbol}' ))

	if not yAxisScale is None or not yAxisScaleSymbol is None:
		if yAxisScale is None:
			yAxisScale = 1
		if yAxisScaleSymbol is None:
			yAxisScaleSymbol = ''
		plt.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{x*yAxisScale:,.0f}{yAxisScaleSymbol}' ))

	if kind == "line":
		
		# Show datapoint for 95 and 94, with number of exams and the datapoint value
		for dot in dots:
			dotx = dot[0]
			dotalign = dot[1]
			dotcolor = dot[2]
			dot_marker = dot[3]
			dot_marker_size = dot[4]
			try:
				doty = df[xvar].value_counts().sort_index()[dotx]

			except KeyError as ke:
				doty = 0

			dotlabel = "{:.1f}".format(dotx) + ": " + locale.format_string("%d", doty, grouping=True)
			plt.scatter(dotx, doty, color=dot[2], marker=dot_marker, s=dot_marker_size)
			if dotalign == 'left':
				dotx = dotx + 0.1
			elif dotalign == 'right':
				dotx = dotx - 0.1
				
			plt.text(dotx, doty, dotlabel, horizontalalignment=dotalign, color=dot[2], bbox=dict(facecolor='white', edgecolor='None', pad=0.1))
#endregion chart



#, dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10))





#region old code
###### OLD CODE ######


# def linechart( df, group_variable = "Class_Exam", xlim = (0, 20), ylim = (0, None), xticks = range(0, 21, 1), xlabel = "Nota de exame", ylabel = "Número de exames", title = None, grid = True, dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) ):
# 	'''
# 	df: dataframe
# 	group_variable: column name to group by
# 	xlim: x-axis limits
# 	ylim: y-axis limits
# 	xticks: x-axis marks
# 	xlabel: x-axis label
# 	ylabel: y-axis label
# 	title: chart title
# 	grid: show horizontal grid lines
# 	dots: list of tuples with (x, align, color, marker, marker_size)
# 	'''
	
# 	df[group_variable].value_counts().sort_index().plot.line()

# 	# x-axis (default: 0 to 200)
# 	# y-axis (default: 0 and end automatically)
# 	plt.xlim(xlim)
# 	plt.ylim(ylim)

# 	# Plot x-axis marks from 0 to 200, step 10
# 	# rotate units in y-axis
# 	plt.xticks(xticks)
# 	plt.yticks(rotation=90)
# 	# Set x-axis label to "Nota de exame"
# 	plt.xlabel(xlabel)

# 	# Set y-axis label to "Número de exames"
# 	# rotate labels 90 degrees in y-axis
# 	plt.ylabel(ylabel, rotation=90)

# 	# Set title to "Distribuição de notas de exame 1a fase, Secundário, 2008-2022"
# 	if title is None:
# 		# Get the first and last year in the dataset
# 		resmin = df['ano'].min()
# 		resmax = df['ano'].max()
# 		if resmin != resmax:
# 			title = "Anos " + str(resmin) + " a " + str(resmax)
# 		else:
# 			title = "Ano " + str(resmin)
		
# 		# Get list of distinct values for Fase
# 		resmin = df['Fase'].min()
# 		resmax = df['Fase'].max()
# 		if resmin != resmax:
# 			title = title + ", ambas as fases"
# 		else:
# 			title = title + ", fase " + str(resmin)

# 	plt.title(title)

# 	# Set horizontal grid lines
# 	plt.grid(grid)

# 	# Show datapoint for 95 and 94, with number of exams and the datapoint value
# 	for dot in dots:
# 		dotx = dot[0]
# 		dotalign = dot[1]
# 		dotcolor = dot[2]
# 		dot_marker = dot[3]
# 		dot_marker_size = dot[4]
# 		try:
# 			doty = df[group_variable].value_counts().sort_index()[dotx]
# 		except KeyError as ke:
# 			doty = 0

# 		dotlabel = "{:.1f}".format(dotx) + ": " + locale.format_string("%d", doty, grouping=True)
# 		plt.scatter(dotx, doty, color=dot[2], marker=dot_marker, s=dot_marker_size)
# 		if dotalign == 'left':
# 			dotx = dotx + 0.1
# 		elif dotalign == 'right':
# 			dotx = dotx - 0.1
			
# 		#plt.annotate(dotlabel, xy=(dotx, doty), xytext=(dotx, doty), horizontalalignment=dotalign, color=dot[2], bbox=dict(facecolor='white', edgecolor='None', pad=0.5))
# 		plt.text(dotx, doty, dotlabel, horizontalalignment=dotalign, color=dot[2], bbox=dict(facecolor='white', edgecolor='None', pad=0.1))
# 		#plt.text(95, res95, 'HELLO')

# 	plt.show()

def barchart_nseries(df, xAxis='ano', dataSeries='Fase', values='count', title=None, xlabel=None, ylabel=None, legendlabel=None, grid=True, stacked=False, colormap='Set3', normalize=False):
	# Pivot the DataFrame to create the data for the stacked bar chart

	#dfAgreg = df.groupby([index, columns]).size(normalize=True).reset_index(name=values)
	#pivot_df = dfAgreg.pivot(index=index, columns=columns, values=values)

	df_grouped = df.groupby(xAxis)[dataSeries].value_counts(normalize=normalize).unstack(dataSeries)
	#print(df_grouped)

	# df_grouped = df.groupby('ano')['Fase'].value_counts(normalize=False).unstack('Fase')
	# print(df_grouped)
	# df_grouped = df.groupby('ano')['Fase'].value_counts(normalize=True).unstack('Fase')
	# print(df_grouped)
	#df_grouped.plot.bar(stacked=True)
	# pivot_df.plot(kind='bar', stacked=True, colormap='Set3')

	df_grouped.plot(kind='bar', stacked=stacked, colormap=colormap)


	# Plot the stacked bar chart
	
	if xlabel is None:
		xlabel = xAxis.capitalize()
	if ylabel is None:
		ylabel = 'Contagem'
	if legendlabel is None:
		legendlabel = dataSeries.capitalize()
	if title is None:
		title = "Exames por " + xAxis.capitalize() + " e por " + dataSeries.capitalize()


	plt.xlabel(xlabel=xlabel)
	plt.ylabel(ylabel=ylabel)
	plt.title( label=title, loc='center', pad=20, fontsize=12, fontweight='bold')
	plt.legend(title=legendlabel)
	plt.grid(grid)

	plt.show()

# def barchart( df, group_variable = "Class_Exam", xlabel = "Nota de exame", ylabel = "Número de exames", title = None, grid = True, dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) ):
# 	'''
# 	df: dataframe
# 	group_variable: column name to group by
# 	xlim: x-axis limits
# 	ylim: y-axis limits
# 	xticks: x-axis marks
# 	xlabel: x-axis label
# 	ylabel: y-axis label
# 	title: chart title
# 	grid: show horizontal grid lines
# 	dots: list of tuples with (x, align, color, marker, marker_size)
# 	'''
	
# 	df[group_variable].value_counts().sort_index().plot.bar()

# 	# x-axis (default: 0 to 200)
# 	# y-axis (default: 0 and end automatically)
# 	#plt.xlim(xlim[1])
# 	#plt.ylim(ylim[1])

# 	# Plot x-axis marks from 0 to 200, step 10
# 	# rotate units in y-axis
# 	#plt.xticks(xticks)
# 	#plt.yticks(rotation=90)
# 	# Set x-axis label to "Nota de exame"
# 	plt.xlabel(xlabel)

# 	# Set y-axis label to "Número de exames"
# 	# rotate labels 90 degrees in y-axis
# 	plt.ylabel(ylabel, rotation=90)

# 	# Set title to "Distribuição de notas de exame 1a fase, Secundário, 2008-2022"
# 	if title is None:
# 		# Get the first and last year in the dataset
# 		resmin = df['ano'].min()
# 		resmax = df['ano'].max()
# 		if resmin != resmax:
# 			title = "Anos " + str(resmin) + " a " + str(resmax)
# 		else:
# 			title = "Ano " + str(resmin)
		
# 		# Get list of distinct values for Fase
# 		resmin = df['Fase'].min()
# 		resmax = df['Fase'].max()
# 		if resmin != resmax:
# 			title = title + ", ambas as fases"
# 		else:
# 			title = title + ", fase " + str(resmin)

# 	plt.title(title)

# 	# Set horizontal grid lines
# 	plt.grid(grid)

# 	# Show datapoint for 95 and 94, with number of exams and the datapoint value

# 	plt.show()

# def chart_bar(df, xvar='Class_Exam', xlabel = 'Nota no exame', xlimit=(0,20), yvar='count', ylabel='Número de exames', ylimit=(0, None), zvar='Fase', title=None, grid=True, colormap='Set3', stacked=False, normalize=False):
# 	# Pivot the DataFrame to create the data for the stacked bar chart

# 	#dfAgreg = df.groupby([index, columns]).size(normalize=True).reset_index(name=values)
# 	#pivot_df = dfAgreg.pivot(index=index, columns=columns, values=values)

# 	df_grouped = df.groupby(xvar)[zvar].value_counts(normalize=normalize).unstack(zvar)
# 	#print(df_grouped)

# 	print(df_grouped)
# 	# df_grouped = df.groupby('ano')['Fase'].value_counts(normalize=False).unstack('Fase')
# 	# print(df_grouped)
# 	# df_grouped = df.groupby('ano')['Fase'].value_counts(normalize=True).unstack('Fase')
# 	# print(df_grouped)
# 	#df_grouped.plot.bar(stacked=True)
# 	# pivot_df.plot(kind='bar', stacked=True, colormap='Set3')

# from ImportUtils import get_dfAll_from_Parquet

# dfAll = get_dfAll_from_Parquet()
# #chart_bar(dfAll, xvar='Class_Exam', yvar='count', zvar='Fase')

# x = dfAll[dfAll['Class_Exam'] < 0]

# print(x[['ano', 'Fase', 'Escola', 'Class_Exam']])

		  

# def zz():
# 	# df_grouped = np.dataframe

# 	# df_grouped.plot(kind='bar', stacked=stacked, colormap=colormap)


# 	# # Plot the stacked bar chart
	
# 	# if xlabel is None:
# 	# 	xlabel = xvar.capitalize()
# 	# if ylabel is None:
# 	# 	ylabel = 'Contagem'
# 	# if legendlabel is None:
# 	# 	legendlabel = zvar.capitalize()
# 	# if title is None:
# 	# 	title = "Exames por " + xvar.capitalize() + " e por " + zvar.capitalize()


# 	# plt.xlabel(xlabel=xlabel)
# 	# plt.ylabel(ylabel=ylabel)
# 	# plt.title( label=title, loc='center', pad=20, fontsize=12, fontweight='bold')
# 	# plt.legend(title=legendlabel)
# 	# plt.grid(grid)

# 	# plt.show()
# 	pass


#endregion oldcode



# from ImportUtils import get_dfAll_from_Parquet
# dfAllFase1 = get_dfAll_from_Parquet()


# dfCountYears = dfAllFase1[["Covid", "ano"]].groupby('Covid').nunique("ano")
# dfCountExamsPerYear = dfAllFase1.groupby(['Class_Exam_Rounded','Covid']).size().reset_index(name='count')
# dfCountExamsPerYear = dfCountExamsPerYear.merge(dfCountYears, on='Covid', how='left')
# dfCountExamsPerYear['count_per_year'] = dfCountExamsPerYear['count'] / dfCountExamsPerYear['ano']

# df2CountExamsPerYear = dfCountExamsPerYear[['Class_Exam_Rounded', 'Covid', 'count_per_year']]

# #dfCountExamsPerYear = dfCountExamsPerYear.pivot(index='Class_Exam_Rounded', columns='Covid', values='count_per_year')

# print(df2CountExamsPerYear[0:5])

# chart(kind="bar", df=df2CountExamsPerYear, xvar="Class_Exam_Rounded", yvar="count_per_year", zvar = "Covid")

# plt.show()


import pandas as pd
from ImportUtils import vprint, vprint_time

import matplotlib.pyplot as plt
import locale

# Set the locale to your system's default (for the desired thousands separator)
locale.setlocale(locale.LC_ALL, 'pt_PT')



def linechart( df, group_variable = "Class_Exam", xlim = (0, 20), ylim = (0, None), xticks = range(0, 21, 1), \
			  xlabel = "Nota de exame", ylabel = "Número de exames", title = None, grid = True, \
			  dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) ):
	'''
	df: dataframe
	group_variable: column name to group by
	xlim: x-axis limits
	ylim: y-axis limits
	xticks: x-axis marks
	xlabel: x-axis label
	ylabel: y-axis label
	title: chart title
	grid: show horizontal grid lines
	dots: list of tuples with (x, align, color, marker, marker_size)
	'''
	
	df[group_variable].value_counts().sort_index().plot.line()

	# x-axis (default: 0 to 200)
	# y-axis (default: 0 and end automatically)
	plt.xlim(xlim)
	plt.ylim(ylim)

	# Plot x-axis marks from 0 to 200, step 10
	# rotate units in y-axis
	plt.xticks(xticks)
	plt.yticks(rotation=90)
	# Set x-axis label to "Nota de exame"
	plt.xlabel(xlabel)

	# Set y-axis label to "Número de exames"
	# rotate labels 90 degrees in y-axis
	plt.ylabel(ylabel, rotation=90)

	# Set title to "Distribuição de notas de exame 1a fase, Secundário, 2008-2022"
	if title is None:
		# Get the first and last year in the dataset
		resmin = df['ano'].min()
		resmax = df['ano'].max()
		if resmin != resmax:
			title = "Anos " + str(resmin) + " a " + str(resmax)
		else:
			title = "Ano " + str(resmin)
		
		# Get list of distinct values for Fase
		resmin = df['Fase'].min()
		resmax = df['Fase'].max()
		if resmin != resmax:
			title = title + ", ambas as fases"
		else:
			title = title + ", fase " + str(resmin)

	plt.title(title)

	# Set horizontal grid lines
	plt.grid(grid)

	# Show datapoint for 95 and 94, with number of exams and the datapoint value
	for dot in dots:
		dotx = dot[0]
		dotalign = dot[1]
		dotcolor = dot[2]
		dot_marker = dot[3]
		dot_marker_size = dot[4]
		try:
			doty = df[group_variable].value_counts().sort_index()[dotx]
		except KeyError as ke:
			doty = 0

		dotlabel = "{:.1f}".format(dotx) + ": " + locale.format_string("%d", doty, grouping=True)
		plt.scatter(dotx, doty, color=dot[2], marker=dot_marker, s=dot_marker_size)
		if dotalign == 'left':
			dotx = dotx + 0.1
		elif dotalign == 'right':
			dotx = dotx - 0.1
			
		#plt.annotate(dotlabel, xy=(dotx, doty), xytext=(dotx, doty), horizontalalignment=dotalign, color=dot[2], bbox=dict(facecolor='white', edgecolor='None', pad=0.5))
		plt.text(dotx, doty, dotlabel, horizontalalignment=dotalign, color=dot[2], bbox=dict(facecolor='white', edgecolor='None', pad=0.1))
		#plt.text(95, res95, 'HELLO')

	plt.show()




def barchart_nseries(df, xAxis='ano', dataSeries='Fase', values='count', title=None, xlabel=None, ylabel=None, \
					   legendlabel=None, grid=True, stacked=False, colormap='Set3', normalize=False):
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


def barchart( df, group_variable = "Class_Exam", \
			  xlabel = "Nota de exame", ylabel = "Número de exames", title = None, grid = True, \
			  dots=((9.4, 'right', 'red', 'x', 10), (9.5, 'left', 'green', 'x', 10), (20, 'right', 'blue', 'x', 10)) ):
	'''
	df: dataframe
	group_variable: column name to group by
	xlim: x-axis limits
	ylim: y-axis limits
	xticks: x-axis marks
	xlabel: x-axis label
	ylabel: y-axis label
	title: chart title
	grid: show horizontal grid lines
	dots: list of tuples with (x, align, color, marker, marker_size)
	'''
	
	df[group_variable].value_counts().sort_index().plot.bar()

	# x-axis (default: 0 to 200)
	# y-axis (default: 0 and end automatically)
	#plt.xlim(xlim[1])
	#plt.ylim(ylim[1])

	# Plot x-axis marks from 0 to 200, step 10
	# rotate units in y-axis
	#plt.xticks(xticks)
	#plt.yticks(rotation=90)
	# Set x-axis label to "Nota de exame"
	plt.xlabel(xlabel)

	# Set y-axis label to "Número de exames"
	# rotate labels 90 degrees in y-axis
	plt.ylabel(ylabel, rotation=90)

	# Set title to "Distribuição de notas de exame 1a fase, Secundário, 2008-2022"
	if title is None:
		# Get the first and last year in the dataset
		resmin = df['ano'].min()
		resmax = df['ano'].max()
		if resmin != resmax:
			title = "Anos " + str(resmin) + " a " + str(resmax)
		else:
			title = "Ano " + str(resmin)
		
		# Get list of distinct values for Fase
		resmin = df['Fase'].min()
		resmax = df['Fase'].max()
		if resmin != resmax:
			title = title + ", ambas as fases"
		else:
			title = title + ", fase " + str(resmin)

	plt.title(title)

	# Set horizontal grid lines
	plt.grid(grid)

	# Show datapoint for 95 and 94, with number of exams and the datapoint value

	plt.show()


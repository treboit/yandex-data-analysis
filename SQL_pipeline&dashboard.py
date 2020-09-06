# !/usr/bin/python
# -*- coding: utf-8 -*-

import sys

import getopt

from datetime import datetime

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import plotly.graph_objs as go

import pandas as pd

from sqlalchemy import create_engine

if __name__ == "__main__":

	unixOptions = "s:e"  
	gnuOptions = ["start_dt=", "end_dt="]

	fullCmdArguments = sys.argv
	argumentList = fullCmdArguments[1:]

	try:  
		arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
	except getopt.error as err:  
		print (str(err))
		sys.exit(2)

	start_dt = '18:00:00 2019-09-24'
	end_dt = '19:00:00 2019-09-24'

	for currentArgument, currentValue in arguments:  
		if currentArgument in ("-s", "--start_dt"):
			start_dt = currentValue                                   
		elif currentArgument in ("-e", "--end_dt"):
			end_dt = currentValue  

	db_config = {'user': 'my_user',
				'pwd': 'my_user_password',
				'host': 'localhost',
				'port': 5432,
				'db': 'zen'}   
	connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
															 db_config['pwd'],
															 db_config['host'],
															 db_config['port'],
															 db_config['db'])
	 #запрашиваем сырые данные
	engine = create_engine(connection_string)    

	query = ''' SELECT *
				,TO_TIMESTAMP(ts/1000) AT TIME ZONE 'Etc/UTC' as dt
			FROM log_raw
			WHERE TO_TIMESTAMP(ts/1000) AT TIME ZONE 'Etc/UTC'  BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
		'''.format(start_dt, end_dt)

	raw = pd.io.sql.read_sql(query, con = engine)
	
	pd.to_datetime(raw['dt']).dt.round('min')

	dash_visits = raw.groupby(['item_topic', 'source_topic', 'age_segment', 'dt']).agg({'event':'count'})
	dash_visits.columns = ['visits']
	
	dash_engagement = raw.groupby(['dt', 'item_topic', 'event', 'age_segment']).agg({'user_id':'nunique'})
	dash_engagement.columns = ['unique_users']

	dash_visits = dash_visits.fillna(0).reset_index()
	dash_engagement = dash_engagement.fillna(0).reset_index()   

	tables = {'dash_visits': dash_visits, 
			  'dash_engagement': dash_engagement}

	for table_name, table_data in tables.items():
		query = '''
				DELETE FROM {} WHERE dt BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
				'''.format(table_name, start_dt, end_dt)
		engine.execute(query)

		table_data.to_sql(name = table_name, con = engine, if_exists = 'append', index = False)

	for table in [dash_visits, dash_engagement]:
		table['dt'] = pd.to_datetime(table['dt'])

	# лэйаут
	external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
	app = dash.Dash(__name__, external_stylesheets=external_stylesheets, compress=False)
	app.layout = html.Div(children=[  
		#заголовок и пояснения
		html.H1(children = 'Заголовок дашборда'),
		html.Br(),  
		html.Label('Пояснение к дашборду'),    
		html.Br(),
		#input
		html.Div([
			#фильтры 1-2
			html.Div([
				#фильтр1
				html.Div([
					html.Label('Выбор даты и времени'),
					dcc.DatePickerRange(
						start_date = raw['dt'].min(),
	                	end_date = raw['dt'].max(),
	                	display_format = 'YYYY-MM-DD',
	                	id = 'dt_selector')
				]),
				#фильтр2
				html.Div([
					html.Label('Возрастные категории'),
					dcc.Dropdown(
						options = [{'label': x, 'value': x} for x in raw['age_segment'].unique()],
						value = raw['age_segment'].unique().tolist(),
						multi = True,
						id = 'age-dropdown')
				]),

			], className = 'six columns'),
			#фильтр 3
			html.Div([
				html.Label('Темы карточек'),
				dcc.Dropdown(
					options = [{'label': x, 'value': x} for x in raw['item_topic'].unique()],
						value = raw['item_topic'].unique().tolist(),
						multi = True,
						id = 'item-topic-dropdown')	
			], className = 'six columns'),
		], className = 'row'),
		#output
		html.Div([
			#график 1
			html.Div([
				html.Label('История событий по темам карточек'),
	            dcc.Graph(
	                id = 'history-absolute-visits',
	                style = {'height': '50vw'},
	            ),
			], className = 'six columns'),
			#графики 2-3
			html.Div([
				#график 2
				html.Div([
					html.Label('Разбивка событий по темам источников'),
	            	dcc.Graph(
		                id = 'pie-visits',
		                style = {'height': '25	vw'},
		            ),
				], className = 'six columns'),
				#график 3
				html.Div([
					html.Label('График средней глубины взаимодействия'),
	            	dcc.Graph(
		                id = 'engagement-graph',
		                style = {'height': '25	vw'},
		            ),
				], className = 'six columns'),

			], className = 'six columns'),
			
		], className = 'row'),
	])

	#логика дашборда
	@app.callback(
		[Output('history-absolute-visits', 'figure'),
		 Output('pie-visits', 'figure'),
		 Output('engagement-graph', 'figure'),
		],
		[Input('dt_selector', 'start_date'),
		 Input('dt_selector', 'end_date'),
		 Input('age-dropdown', 'value'),
		 Input('item-topic-dropdown', 'value'),
		])

	#фильтрация и графики
	def update_figures(start_date, end_date, selected_ages, selected_item_topics):

		dash_visits = dash_visits.query('item_topic.isin(@selected_item_topics) and \
   										dt >= @start_date and dt <= @end_date \
   										and age_segment.isin(@selected_ages)')
		
		dash_visits_by_item_topic = (dash_visits.groupby(['item_topic', 'dt'])
								.agg({'visits': 'count'})
								.reset_index()
						  		)

		history_absolute_visits = []
		for item_topic in dash_visits_by_item_topic['item_topic'].unique():
			history_absolute_visits += [go.Scatter(x = dash_visits_by_item_topic.query('item_topic == @item_topic')['dt'],
									   			y = dash_visits_by_item_topic.query('item_topic == @item_topic')['visits'],
												mode = 'lines',
												stackgroup = 'one',
												name = item_topic)]

		dash_visits_by_source_topic = dash_visits.groupby('source_topic').agg({'visits': 'count'}).reset_index()

		pie_visits = go.Pie(labels = dash_visits_by_source_topic['source_topic'], 
         							values = dash_visits_by_source_topic['visits'])

		dash_engagement = dash_engagement.query('item_topic.isin(@selected_item_topics) and \
   												dt >= @start_date and dt <= @end_date \
   												and age_segment.isin(@selected_ages)')

		dash_engagement_by_event = dash_engagement.groupby('event').agg({'unique_users': 'mean'}).reset_index()
		dash_engagement_by_event['avg_unique_users'] = (dash_engagement_by_event['unique_users'] /
                                                dash_engagement_by_event['unique_users'].max())
		dash_engagement_by_event = dash_engagement_by_event.sort_values('avg_unique_users', ascending = False)


		engagement_graph = go.Bar(x = dash_engagement_by_event['event'],
         						y = dash_engagement_by_event['avg_unique_users'])


		#формируем результат для отображения
		return (
				{
					'data': history_absolute_visits,
					'layout': go.Layout(xaxis = {'title': 'Время'},
										yaxis = {'title': 'Количество событий'})
				 },
				{
					'data': pie_visits,
				 },             
				{
					'data': engagement_graph,
					'layout': go.Layout(yaxis = {'title': 'Доля видов взаимодействия'})
				 },
		)  

	app.run_server(debug = True, host='0.0.0.0')
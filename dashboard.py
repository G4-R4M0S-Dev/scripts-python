import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dash_table import FormatTemplate
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
import os
from threading import Timer
import webbrowser


trabalhador = pd.read_csv("csv/trabalhador.csv", sep=',',decimal=',', converters={'cpf': str})
movimentos = pd.read_csv("csv/movimentos_fopag.csv", sep=',',decimal=',')

trabalhador = trabalhador.sort_values(by=['registro','nome'])
movimentos["data"] = movimentos["mes"].astype(str) + '/' + movimentos["ano"].astype(str)

holerite = movimentos
holerite.loc[holerite['natureza'] == 'D', 'valor'] = holerite[holerite['natureza'] == 'D']['valor'] * -1
holerite.drop(holerite[holerite['natureza'] == 'V'].index, inplace=True)
holerite = movimentos.pivot_table(values='valor', index=['evento','nome','natureza'], columns='data',margins=True, margins_name='total',aggfunc=np.sum, fill_value=0).reset_index()
holerite = holerite.sort_values(by=['evento','nome'])

local_trabalho = pd.read_csv("csv/local_trabalho.csv", sep=',')
local_trabalho['codigo'] = local_trabalho['codigo'].astype(float)


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SOLAR])


def table_type(df_column):
    if isinstance(df_column.dtype, pd.DatetimeTZDtype):
        return 'datetime',
    elif (isinstance(df_column.dtype, pd.StringDtype) or
            isinstance(df_column.dtype, pd.BooleanDtype) or
            isinstance(df_column.dtype, pd.CategoricalDtype) or
            isinstance(df_column.dtype, pd.PeriodDtype)):
        return 'text'
    elif (isinstance(df_column.dtype, pd.SparseDtype) or
            isinstance(df_column.dtype, pd.IntervalDtype) or
            isinstance(df_column.dtype, pd.Int8Dtype) or
            isinstance(df_column.dtype, pd.Int16Dtype) or
            isinstance(df_column.dtype, pd.Int32Dtype) or
            isinstance(df_column.dtype, pd.Int64Dtype)):
        return 'numeric'
    else:
        return 'any'


table_trabalhador = dash.dash_table.DataTable(id='table-trabalhador',
                                      columns=[{"name": 'registro', "id": 'registro', 'type': 'numeric'},
                                               {"name": 'nome', "id": 'nome', 'type': 'text'},
                                               {"name": 'cpf', "id": 'cpf', 'type': 'text'},
                                               {"name": 'dtsituacao', "id": 'dtsituacao', 'type': 'datetime'},
                                               {"name": 'dtadmissao', "id": 'dtadmissao', 'type': 'datetime'},
                                               {"name": 'dtdemissao', "id": 'dtdemissao', 'type': 'datetime'},
                                               {"name": 'dtnascimento', "id": 'dtnascimento', 'type': 'datetime'},
                                               {"name": 'sexo', "id": 'sexo', 'type': 'text'},
                                               {"name": 'situacao_nome', "id": 'situacao_nome', 'type': 'text'},
                                               {"name": 'divisao', "id": 'divisao', 'type': 'text'},
                                               {"name": 'subdivisao', "id": 'subdivisao', 'type': 'text'},
                                               {"name": 'vinculo', "id": 'vinculo', 'type': 'text'},
                                               {"name": 'estadocivil', "id": 'estadocivil', 'type': 'text'},
                                               {"name": 'cargoatual', "id": 'cargoatual', 'type': 'text'},
                                               ],
                                      style_cell={'textAlign': 'left'},
                                      page_current= 0,
                                      page_size= 10,
                                      page_action='custom',
                                      filter_action='custom',
                                      filter_query='',
                                      sort_action='custom',
                                      sort_mode='multi',
                                      sort_by=[],
                                      style_table={'overflowX': 'scroll'},
                                      css=[],
                                      )


money = FormatTemplate.money(2)
table_movimentos = dash.dash_table.DataTable(id='table-movimentos',
                                      columns=[dict(name=i, id=i,type='numeric', format=money) if holerite[i].dtypes == np.float64 else dict(name=i, id=i) for i in holerite.columns],
                                      style_cell={'textAlign': 'left'},
                                      page_current= 0,
                                      page_size= 10,
                                      page_action='custom',
                                      filter_action='custom',
                                      filter_query='',
                                      sort_action='custom',
                                      sort_mode='multi',
                                      sort_by=[],
                                      hidden_columns=['natureza'],
                                      style_table={'overflowX': 'scroll'},
                                      css=[{"selector": ".show-hide", "rule": "display: none"}],
                                      style_data_conditional=[
                                                                {
                                                                    'if': {
                                                                        'column_id': 'total',
                                                                    },
                                                                    'color': 'black',
                                                                    'fontWeight': 'bold'
                                                                },
                                                                {
                                                                    'if': {
                                                                        'filter_query': '{evento} contains "total"'
                                                                    },
                                                                    'color': 'black',
                                                                    'fontWeight': 'bold'
                                                                },
                                                                
                                                            ]
                                      )


app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
                dbc.Row([
                    dbc.Card([
                        dbc.CardBody([
                            html.P("Situação: "),
                            dcc.Dropdown(trabalhador.situacao_nome.unique(),id='situacao-dropdown', multi=True)
                        ])
                    ])
                ]),
                dbc.Row([
                    dbc.Card([
                        dbc.CardBody([
                            html.P("Vinculo: "),
                            dcc.Dropdown(trabalhador.vinculo.unique(),id='vinculo-dropdown', multi=True)
                        ])
                    ])
                ]),
                dbc.Row([
                    dbc.Card([
                        dbc.CardBody([
                            html.P("Divisão: "),
                            dcc.Dropdown(trabalhador.divisao.unique(),id='divisao-dropdown', multi=True)
                        ])
                    ])
                ]),
                dbc.Row([
                    dbc.Card([
                        dbc.CardBody([
                            html.P("Subdivisão: "),
                            dcc.Dropdown(trabalhador.subdivisao.unique(),id='subdivisao-dropdown', multi=True)
                        ])
                    ])
                ]),
                dbc.Row([
                    dbc.Card([
                        dbc.CardBody([
                            html.P("Local de Trabalho: "),
                            dcc.Dropdown(local_trabalho.nome.unique(),id='local-trabalho-dropdown', multi=True)
                        ])
                    ])
                ]),
                dbc.Row([
                    dbc.Card([
                        dbc.CardBody([
                            html.P("Cargo: "),
                            dcc.Dropdown(trabalhador.cargoatual.unique(),id='cargo-dropdown', multi=True)
                        ])
                    ])
                ]) 
            
        ], md=2),
        dbc.Col([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                table_trabalhador
                            ])
                        ])
                    ])
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                table_movimentos
                            ])
                        ])
                    ])
                ]),
                
        ], md=10),
    ]),
    
],fluid=True)


operators = [['ge ', '>='],
             ['le ', '<='],
             ['lt ', '<'],
             ['gt ', '>'],
             ['ne ', '!='],
             ['eq ', '='],
             ['contains '],
             ['datestartswith ']]


def split_filter_part(filter_part):
    for operator_type in operators:
        for operator in operator_type:
            if operator in filter_part:
                name_part, value_part = filter_part.split(operator, 1)
                name = name_part[name_part.find('{') + 1: name_part.rfind('}')]

                value_part = value_part.strip()
                v0 = value_part[0]
                if (v0 == value_part[-1] and v0 in ("'", '"', '`')):
                    value = value_part[1: -1].replace('\\' + v0, v0)
                else:
                    try:
                        value = float(value_part)
                    except ValueError:
                        value = value_part

                # word operators need spaces after them in the filter string,
                # but we don't want these later
                return name, operator_type[0].strip(), value

    return [None] * 3


@app.callback(
    
        Output('table-trabalhador', 'data'),
        Output('table-trabalhador', 'page_count'),
        Output('situacao-dropdown', 'options'),
        Output('vinculo-dropdown', 'options'),
        Output('divisao-dropdown', 'options'),
        Output('subdivisao-dropdown', 'options'),
        Output('cargo-dropdown', 'options'),
        Output('local-trabalho-dropdown', 'options')
        # Output('table-movimentos', 'data')
    ,
    
        Input('table-trabalhador', "page_current"),
        Input('table-trabalhador', "page_size"),
        Input('table-trabalhador', 'sort_by'),
        Input('table-trabalhador', 'filter_query'),
        Input('situacao-dropdown', 'value'),
        Input('vinculo-dropdown', 'value'),
        Input('divisao-dropdown', 'value'),
        Input('subdivisao-dropdown', 'value'),
        Input('local-trabalho-dropdown', 'value'),
        Input('cargo-dropdown', 'value')
        
    )

def update_table(page_current, page_size, sort_by, filter, situacao_value,vinculo_value,divisao_value,subdivisao_value,local_trabalho_value,cargo_value):
    filtering_expressions = filter.split(' && ')
    
    dff = trabalhador
    
    if situacao_value is not None:
        if situacao_value != []:
            dff = dff[dff['situacao_nome'].isin(situacao_value)]
        else:
            dff = dff[~dff['situacao_nome'].isin(situacao_value)]
    if vinculo_value is not None:    
        if vinculo_value != []:
            dff = dff[dff['vinculo'].isin(vinculo_value)]
        else:
            dff = dff[~dff['vinculo'].isin(vinculo_value)]
    if divisao_value is not None:    
        if divisao_value != []:
            dff = dff[dff['divisao'].isin(divisao_value)]
        else:
            dff = dff[~dff['divisao'].isin(divisao_value)]
    if subdivisao_value is not None:    
        if subdivisao_value != []:
            dff = dff[dff['subdivisao'].isin(subdivisao_value)]
        else:
            dff = dff[~dff['subdivisao'].isin(subdivisao_value)]
    if local_trabalho_value is not None:    
        if local_trabalho_value != []:
            values = local_trabalho[local_trabalho['nome'].isin(local_trabalho_value)]
            dff = dff[dff['cod_local_trabalho'].isin(list(values.codigo))]
        else:
            values = local_trabalho[local_trabalho['nome'].isin(local_trabalho_value)]
            dff = dff[~dff['cod_local_trabalho'].isin(list(values.codigo))]
    if cargo_value is not None:    
        if cargo_value != []:
            dff = dff[dff['cargoatual'].isin(cargo_value)]
        else:
            dff = dff[~dff['cargoatual'].isin(cargo_value)]
    
    page_count = int(dff.shape[0]/10+1)
    
    for filter_part in filtering_expressions:
        col_name, operator, filter_value = split_filter_part(filter_part)

        if operator in ('eq', 'ne', 'lt', 'le', 'gt', 'ge'):
            # these operators match pandas series operator method names
            dff = dff.loc[getattr(dff[col_name], operator)(filter_value)]
        elif operator == 'contains':
            dff = dff.loc[dff[col_name].str.contains(filter_value)]
        elif operator == 'datestartswith':
            # this is a simplification of the front-end filtering logic,
            # only works with complete fields in standard format
            dff = dff.loc[dff[col_name].str.startswith(filter_value)]

    if len(sort_by):
        dff = dff.sort_values(
            [col['column_id'] for col in sort_by],
            ascending=[
                col['direction'] == 'asc'
                for col in sort_by
            ],
            inplace=False
        )

    page = page_current
    size = page_size
    
    situacao_nome_options = dff.situacao_nome.unique()
    vinculo_options = dff.vinculo.unique()
    divisao_options = dff.divisao.unique()
    subdivisao_options = dff.subdivisao.unique()
    cargoatual_options = dff.cargoatual.unique()
    
    local_trabalho_options = dff.cod_local_trabalho.unique()
    local_trabalho_options = local_trabalho[local_trabalho.codigo.isin(local_trabalho_options)]
    local_trabalho_options = local_trabalho_options.nome.unique()
    
    
    
    return dff.iloc[page * size: (page + 1) * size].to_dict('records'), page_count, situacao_nome_options,vinculo_options, divisao_options, subdivisao_options, cargoatual_options, local_trabalho_options

@app.callback(
    
        Output('table-movimentos', 'data'),
        Output('table-movimentos', 'page_count')
    ,
    
        Input('table-movimentos', "page_current"),
        Input('table-movimentos', "page_size"),
        Input('table-movimentos', 'sort_by'),
        Input('table-movimentos', 'filter_query'),
        Input('table-trabalhador', 'active_cell'),
        Input('table-trabalhador', 'data')
        
        
    )

def update_table(page_current, page_size, sort_by, filter, active_cell, data_tabela_trabalhador):
    filtering_expressions = filter.split(' && ')
    
    if active_cell:
        registro = data_tabela_trabalhador[active_cell['row']]['registro']
        movimentos_do_registro = movimentos.loc[movimentos['registro'] == registro]
        holerite_registro = movimentos_do_registro.pivot_table(values='valor', index=['evento','nome','natureza'], columns='data',margins=True, margins_name='total',aggfunc=np.sum, fill_value=0).reset_index()
        holerite_registro = holerite_registro.sort_values(by=['evento','nome'])
        dff = holerite_registro
    else:
        dff = holerite
    
    page_count = int(dff.shape[0]/10+1)
    
    for filter_part in filtering_expressions:
        col_name, operator, filter_value = split_filter_part(filter_part)

        if operator in ('eq', 'ne', 'lt', 'le', 'gt', 'ge'):
            # these operators match pandas series operator method names
            dff = dff.loc[getattr(dff[col_name], operator)(filter_value)]
        elif operator == 'contains':
            dff = dff.loc[dff[col_name].str.contains(filter_value)]
        elif operator == 'datestartswith':
            # this is a simplification of the front-end filtering logic,
            # only works with complete fields in standard format
            dff = dff.loc[dff[col_name].str.startswith(filter_value)]

    if len(sort_by):
        dff = dff.sort_values(
            [col['column_id'] for col in sort_by],
            ascending=[
                col['direction'] == 'asc'
                for col in sort_by
            ],
            inplace=False
        )

    page = page_current
    size = page_size
    
    
    
    return dff.iloc[page * size: (page + 1) * size].to_dict('records'), page_count

def open_browser():
    if not os.environ.get("WERKZEUG_RUN_MAIN"):
        webbrowser.get('C:/Program Files/Google/Chrome/Application/chrome.exe %s --app=http://127.0.0.1:8050/').open_new('http://127.0.0.1:8050/')
        
if __name__ == '__main__':
    Timer(1, open_browser).start()
    app.run_server(debug=True)


import os
from databricks import sql
import pandas as pd
import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from databricks.sdk.core import Config, oauth_service_principal  # Added import
from dash.dependencies import ALL  # Added import for ALL
from flask import request

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

def sqlQuery(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    cfg = Config()  # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# Define a default layout to ensure the app always has a valid layout
app.layout = html.Div([
    dcc.Store(id='email-store'),
    html.Div(
        [
            dbc.Button("☰", id="sidebar-toggle", color="primary", className="me-1", style={
                'position': 'absolute',
                'top': '18px',
                'left': '10px',
                'z-index': '1001',
                'background-color': 'white',
                'color': 'black'
            }),
            html.Img(
                src="assets/RBC.png",  # Corrected path to the image
                style={
                    'height': '80px',
                    'position': 'absolute',
                    'top': '-1px',
                    'left': '60px',
                    'z-index': '1001'
                }
            ),
            html.H1(id='email-display', style={'color': 'white', 'text-align': 'center', 'margin-left': '100px'}),
            dbc.Button("i", id="info-button", color="info", className="me-1", style={
                'position': 'absolute',
                'top': '23px',
                'right': '10px',
                'z-index': '1001',
                'border-radius': '50%',
                'width': '30px',
                'height': '30px',
                'padding': '0',
                'text-align': 'center',
                'line-height': '30px',
                'background-color': 'white',  # Set background color to white
                'color': 'black'  # Set text color to black
            }),
        ],
        style={
            'background-color': '#007BFF',
            'padding': '10px',
            'width': '100%',
            'position': 'fixed',
            'top': '0',
            'left': '0',
            'z-index': '1000'
        }
    ),
    dbc.Modal(
        [
            dbc.ModalHeader("About This App"),
            dbc.ModalBody("This app enables management of access permissions for users and teams. "
                          "Users can view Access Model Tables, including their team permissions, and add or remove users from the model."),
            dbc.ModalFooter(
                dbc.Button("Close", id="close-info-modal", className="ms-auto", n_clicks=0)
            ),
        ],
        id="info-modal",
        is_open=False,
    ),
    html.Div([
        html.Div(  # Removed the redundant dbc.Button here
            id="sidebar",
            children=[
                html.H4("Menu", style={'text-align': 'center'}),
                html.Hr(),
                dbc.Nav(
                    [
                        dbc.NavLink("Access Model Tables", href="#", id="tables-link", active="exact"),
                        dbc.NavLink("Add User to Model", href="#", id="add-user-link", active="exact")
                    ],
                    vertical=True,
                    pills=True
                )
            ],
            style={
                'position': 'fixed',
                'top': '0',
                'left': '-250px',
                'width': '250px',
                'height': '100%',
                'background-color': '#f8f9fa',
                'padding': '15px',
                'transition': 'left 0.3s ease-in-out',
                'z-index': '999'
            }
        )
    ]),
    html.Div(id='main-layout', style={'margin-top': '60px'})  # Dynamic content container
])

@app.callback(
    [Output('email-display', 'children'),
     Output('email-store', 'data')],  # Store the email in dcc.Store
    Input('main-layout', 'children')  # Trigger on page load or content change
)
def display_email(_):
    """Retrieve and display the email from the 'X-Forwarded-Email' header."""
    email = request.headers.get("X-Forwarded-Email", "Email not found")
    return f"Access Model: {email}", email  # Return email for storage

@app.callback(
    Output('main-layout', 'children'),
    [Input('tables-link', 'n_clicks'), Input('add-user-link', 'n_clicks')]
)
def render_page(tables_click, add_user_click):
    ctx = dash.callback_context
    if not ctx.triggered or ctx.triggered[0]['prop_id'].split('.')[0] == 'tables-link':
        # Default page: "Access Model Tables"
        return dbc.Container([
            dbc.Row([
                dbc.Col([
                    dcc.Tabs(id='tabs', value='tab1', children=[
                        dcc.Tab(label='Report Users', value='tab1'),
                        dcc.Tab(label='Team Grouping', value='tab2'),
                        dcc.Tab(label='Bridge Mandate Team Access', value='tab3'),
                        dcc.Tab(label='My Team Permissions', value='tab4')
                    ]),
                    dcc.Loading(
                        id="loading-icon",
                        type="circle",
                        children=html.Div(id='tab-content'),
                        style={'margin-top': '50px'}  # Lower the loading icon further
                    ),
                ], width=12)
            ])
        ], fluid=True)

    elif ctx.triggered[0]['prop_id'].split('.')[0] == 'add-user-link':
        return dbc.Container([
            dbc.Row([
                dbc.Col(html.Div(id='add-user-output', className='mt-3'), width=12)  # Move alert container to the top
            ]),
            dbc.Row([
                dbc.Col(html.H3("Add Manager to Worker Permission", style={'margin-top': '20px', 'text-align': 'center'}), width=6),
                dbc.Col(html.H3("Add User to Team Permission", style={'margin-top': '20px', 'text-align': 'center'}), width=6)
            ]),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Manager User", style={'font-weight': 'bold'}),
                    dcc.Dropdown(
                        id='manager-user-dropdown',
                        placeholder='Select Manager Email',
                        options=[],
                        style={'margin-bottom': '15px'}
                    ),
                    dbc.Label("Worker User", style={'font-weight': 'bold'}),
                    dcc.Dropdown(
                        id='worker-user-dropdown',
                        placeholder='Select Worker Email',
                        options=[],
                        style={'margin-bottom': '15px'}
                    ),
                    dbc.Button("Add Access User", id='add-access-user-btn', color='primary', className='mt-3', style={'width': '100%'}),
                    dbc.Button("Delete Permission", id='delete-permission-btn', color='danger', className='mt-3', style={'width': '100%'})
                ], width=6),
                dbc.Col([
                    dbc.Label("Worker Email", style={'font-weight': 'bold'}),
                    dcc.Dropdown(
                        id='worker-name-dropdown',
                        placeholder='Select Worker Email',
                        options=[],
                        style={'margin-bottom': '15px'}
                    ),
                    dbc.Label("Team Name", style={'font-weight': 'bold'}),
                    dcc.Dropdown(
                        id='team-name-dropdown',
                        placeholder='Select Team Name',
                        options=[],
                        style={'margin-bottom': '15px'}
                    ),
                    dbc.Button("Add User to Team", id='add-user-to-team-btn', color='primary', className='mt-3', style={'width': '100%'}),
                    dbc.Button("Delete Team Permission", id='delete-team-permission-btn', color='danger', className='mt-3', style={'width': '100%'})
                ], width=6)
            ]),
            dbc.Row([
                dbc.Col(
                    dcc.Loading(
                        type="circle",
                        children=dag.AgGrid(
                            id='user-grid',
                            columnDefs=[
                                {"headerName": "ID", "field": "id", "flex": 0.5},
                                {"headerName": "Manager Email", "field": "manager_name", "flex": 2, "filter": "agTextColumnFilter"},
                                {"headerName": "Worker Email", "field": "worker_name", "flex": 2},
                                {"headerName": "Timestamp", "field": "timestamp", "flex": 1}
                            ],
                            rowData=[],
                            defaultColDef={
                                "sortable": True,
                                "filter": True,
                                "resizable": True,
                                "cellStyle": {"fontSize": "12px"}
                            },
                            dashGridOptions={"rowSelection": "single"},
                            style={'height': 'calc(100vh - 120px)', 'width': '100%'}
                        )
                    ),
                    width=6,
                    style={'padding-right': '10px'}
                ),
                dbc.Col(
                    dcc.Loading(
                        type="circle",
                        children=dag.AgGrid(
                            id='team-grid',
                            columnDefs=[
                                {"headerName": "ID", "field": "id", "flex": 0.5},
                                {"headerName": "Worker Email", "field": "worker_name", "flex": 2},
                                {"headerName": "Team Name", "field": "team_name", "flex": 1.5},
                                {"headerName": "Timestamp", "field": "timestamp", "flex": 1}
                            ],
                            rowData=[],
                            defaultColDef={
                                "sortable": True,
                                "filter": True,
                                "resizable": True,
                                "cellStyle": {"fontSize": "12px"}
                            },
                            dashGridOptions={"rowSelection": "single"},
                            style={'height': 'calc(100vh - 120px)', 'width': '100%'}
                        )
                    ),
                    width=6,
                    style={'padding-left': '10px'}
                )
            ])
        ], fluid=True)
    else:
        return html.Div("Page not found.")

@app.callback(
    Output('sidebar', 'style'),
    Input('sidebar-toggle', 'n_clicks'),
    State('sidebar', 'style')
)
def toggle_sidebar(n_clicks, sidebar_style):
    if n_clicks and sidebar_style['left'] == '-250px':
        sidebar_style['left'] = '0px'
    elif n_clicks:
        sidebar_style['left'] = '-250px'
    return sidebar_style

@app.callback(
    Output('tab-content', 'children'),
    [Input('tabs', 'value'),
     Input('email-store', 'data')]  # Use the stored email as input
)
def render_table(tab, email):
    if tab == 'tab1':
        query = "SELECT UserKey, EmployeeNumber, internalemailaddress as EmailAddress, fullname as Name FROM minerva_prod.goldaccessmodel.dimreportuser"
    elif tab == 'tab2':
        query = "SELECT TeamGroupingKey, TeamName, Office, City FROM minerva_prod.goldaccessmodel.teamgrouping"
    elif tab == 'tab3':
        query = "SELECT * FROM minerva_prod.goldaccessmodel.bridgemandateteamaccess"
    elif tab == 'tab4':  # Handle the new "Own Permission" tab
        query = f"SELECT dimreportuser.UserKey, EmployeeNumber, internalemailaddress as Email, fullname as Name, TeamName FROM minerva_prod.goldaccessmodel.dimreportuser INNER JOIN minerva_prod.goldaccessmodel.bridgeuserteam on dimreportuser.UserKey = bridgeuserteam.UserKey INNER JOIN minerva_prod.goldaccessmodel.teamgrouping on teamgrouping.TeamGroupingKey = bridgeuserteam.TeamGroupingKey WHERE internalemailaddress='{email}'"
    else:
        return html.Div("No data available.")

    try:
        table_data = sqlQuery(query)
        return dag.AgGrid(
            id='data-grid',
            columnDefs=[{"headerName": col, "field": col, "flex": 1} for col in table_data.columns],
            rowData=table_data.to_dict('records'),
            defaultColDef={"sortable": True, "filter": True, "resizable": True},
            style={'height': 'calc(100vh - 100px)', 'width': '100%'}  # Adjust height dynamically
        )
    except Exception as e:
        return html.Div(f"An error occurred: {str(e)}")

@app.callback(
    [Output('manager-user-dropdown', 'options'),
     Output('worker-user-dropdown', 'options')],
    Input('add-user-link', 'n_clicks')
)
def populate_user_dropdowns(n_clicks):
    try:
        query = "SELECT DISTINCT internalemailaddress FROM minerva_prod.goldaccessmodel.dimreportuser"
        email_data = sqlQuery(query)
        email_options = [{'label': email, 'value': email} for email in email_data['internalemailaddress']]
        return email_options, email_options
    except Exception as e:
        print(f"An error occurred while fetching email addresses: {str(e)}")
        return [], []

@app.callback(
    [Output('worker-name-dropdown', 'options'),
     Output('team-name-dropdown', 'options')],
    Input('add-user-link', 'n_clicks')
)
def populate_team_dropdowns(n_clicks):
    try:
        # Query for worker emails
        worker_query = "SELECT DISTINCT internalemailaddress FROM minerva_prod.goldaccessmodel.dimreportuser"
        worker_data = sqlQuery(worker_query)
        worker_options = [{'label': email, 'value': email} for email in worker_data['internalemailaddress']]

        # Query for team names
        team_query = "SELECT DISTINCT TeamName FROM minerva_prod.goldaccessmodel.teamgrouping"
        team_data = sqlQuery(team_query)
        team_options = [{'label': team, 'value': team} for team in team_data['TeamName']]

        return worker_options, team_options
    except Exception as e:
        print(f"An error occurred while fetching dropdown data: {str(e)}")
        return [], []

@app.callback(
    [Output('user-grid', 'rowData'),
     Output('team-grid', 'rowData'),
     Output('add-user-output', 'children')],  # Combine outputs into one callback
    [Input('main-layout', 'children'),
     Input('add-access-user-btn', 'n_clicks'),
     Input('delete-permission-btn', 'n_clicks'),
     Input('add-user-to-team-btn', 'n_clicks'),
     Input('delete-team-permission-btn', 'n_clicks')],  # Combine inputs for both grids
    [State('manager-user-dropdown', 'value'),
     State('worker-user-dropdown', 'value'),
     State('worker-name-dropdown', 'value'),
     State('team-name-dropdown', 'value'),
     State('user-grid', 'selectedRows'),
     State('team-grid', 'selectedRows'),
     State('user-grid', 'rowData'),
     State('team-grid', 'rowData')]
)
def manage_grids(page_load, add_user_click, delete_user_click, add_team_click, delete_team_click,
                 manager_user, worker_user, worker_name, team_name,
                 user_selected_rows, team_selected_rows, user_grid_data, team_grid_data):
    ctx = dash.callback_context
    if not ctx.triggered:
        return user_grid_data, team_grid_data, None

    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]

    # Fetch data for both grids on page load
    if triggered_id == 'main-layout':
        try:
            user_query = "SELECT * FROM minerva_dev.accessmodel.managerworkerextension"
            user_data = sqlQuery(user_query)
            team_query = "SELECT * FROM minerva_dev.accessmodel.userteamextension"
            team_data = sqlQuery(team_query)
            return user_data.to_dict('records'), team_data.to_dict('records'), None
        except Exception as e:
            print(f"An error occurred while loading data: {str(e)}")
            return [], [], dbc.Alert(f"An error occurred while loading data: {str(e)}", color="danger")

    # Handle user-grid operations
    if triggered_id == 'add-access-user-btn' and add_user_click:
        if not manager_user or not worker_user:
            return user_grid_data, team_grid_data, dbc.Alert("Manager and Worker fields cannot be empty.", color="warning")
        try:
            query = f"""
                INSERT INTO minerva_dev.accessmodel.managerworkerextension (id, manager_name, worker_name, timestamp)
                SELECT COALESCE(MAX(id), 0) + 1, '{manager_user}', '{worker_user}', CURRENT_TIMESTAMP
                FROM minerva_dev.accessmodel.managerworkerextension
            """
            sqlQuery(query)
            query = "SELECT * FROM minerva_dev.accessmodel.managerworkerextension"
            user_data = sqlQuery(query)
            return user_data.to_dict('records'), team_grid_data, dbc.Alert("User added successfully.", color="success")
        except Exception as e:
            print(f"An error occurred while adding a user: {str(e)}")
            return user_grid_data, team_grid_data, dbc.Alert(f"An error occurred while adding the user: {str(e)}", color="danger")

    elif triggered_id == 'delete-permission-btn' and delete_user_click:
        if user_selected_rows:
            try:
                row_id = user_selected_rows[0]['id']
                query = f"DELETE FROM minerva_dev.accessmodel.managerworkerextension WHERE id = {row_id}"
                sqlQuery(query)
                query = "SELECT * FROM minerva_dev.accessmodel.managerworkerextension"
                user_data = sqlQuery(query)
                return user_data.to_dict('records'), team_grid_data, dbc.Alert(f"Row with ID {row_id} deleted successfully.", color="success")
            except Exception as e:
                print(f"An error occurred while deleting a user: {str(e)}")
                return user_grid_data, team_grid_data, dbc.Alert(f"An error occurred while deleting the row: {str(e)}", color="danger")
        else:
            return user_grid_data, team_grid_data, dbc.Alert("No row selected for deletion.", color="warning")

    # Handle team-grid operations
    elif triggered_id == 'add-user-to-team-btn' and add_team_click:
        try:
            query = f"""
                INSERT INTO minerva_dev.accessmodel.userteamextension (id, worker_name, team_name, timestamp)
                SELECT COALESCE(MAX(id), 0) + 1, '{worker_name}', '{team_name}', CURRENT_TIMESTAMP
                FROM minerva_dev.accessmodel.userteamextension
            """
            sqlQuery(query)
            query = "SELECT * FROM minerva_dev.accessmodel.userteamextension"
            team_data = sqlQuery(query)
            return user_grid_data, team_data.to_dict('records'), dbc.Alert("Team permission added successfully.", color="success")
        except Exception as e:
            print(f"An error occurred while adding a team permission: {str(e)}")
            return user_grid_data, team_grid_data, dbc.Alert(f"An error occurred while adding the team permission: {str(e)}", color="danger")

    elif triggered_id == 'delete-team-permission-btn' and delete_team_click:
        if team_selected_rows:
            try:
                row_id = team_selected_rows[0]['id']
                query = f"DELETE FROM minerva_dev.accessmodel.userteamextension WHERE id = {row_id}"
                sqlQuery(query)
                query = "SELECT * FROM minerva_dev.accessmodel.userteamextension"
                team_data = sqlQuery(query)
                return user_grid_data, team_data.to_dict('records'), dbc.Alert(f"Row with ID {row_id} deleted successfully.", color="success")
            except Exception as e:
                print(f"An error occurred while deleting a team permission: {str(e)}")
                return user_grid_data, team_grid_data, dbc.Alert(f"An error occurred while deleting the row: {str(e)}", color="danger")
        else:
            return user_grid_data, team_grid_data, dbc.Alert("No row selected for deletion.", color="warning")

    return user_grid_data, team_grid_data, None

@app.callback(
    Output("info-modal", "is_open"),
    [Input("info-button", "n_clicks"), Input("close-info-modal", "n_clicks")],
    [State("info-modal", "is_open")]
)
def toggle_info_modal(info_click, close_click, is_open):
    if info_click or close_click:
        return not is_open
    return is_open

if __name__ == "__main__":
    app.run(debug=True)
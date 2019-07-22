#!/usr/bin/python3

import csv
import requests
from bokeh.io import curdoc
import json

import numpy as np

#from frontend import frontend as fe

import seaborn as sns

from datetime import datetime, date

from bokeh.models.annotations import Title
from bokeh.io import curdoc
from bokeh.models import (
    GeoJSONDataSource,
    HoverTool,
    LogColorMapper,
    LinearColorMapper,
    Div,
    DateRangeSlider,
    RangeSlider,
    MultiSelect,
    RadioButtonGroup,
    ColorBar,
    Toggle,
    LogTicker,
    AdaptiveTicker,
    ColumnDataSource,
)

from bokeh.layouts import layout, widgetbox, column
from bokeh.models.tools import WheelZoomTool, PanTool
from bokeh.models.widgets import Button
from bokeh.plotting import figure
from bokeh.tile_providers import CARTODBPOSITRON as tileset

from project_conf import conf as proj_conf
from data_conf import data_conf

def create_update_button(function):
    update_button = Button()
    update_button.label = "Update"
    update_button.on_click(function)

    return update_button

def create_project_selectors():
    def get_selector_contents(selector_name):
        fp = '{}/{}.{}.txt'.format(proj_conf.dropdown_dir, selector_name, proj_conf.environment)
        with open(fp, 'r') as fh:
            reader = csv.reader(fh)

            selector_options = []
            for fk_id, description, value in list(reader)[1:]:
                if int(value) <= 0:
                    continue
                selector_option = "{} ({})".format(description, value)
                selector_options.append(selector_option)

        return selector_options

    selectors = []
    for multi_selector in data_conf.multi_selectors:
        column_name = multi_selector['column_name']
        title = multi_selector['title']

        selector_contents = get_selector_contents(column_name)
        if not selector_contents:
            print('wtf, ', multi_selector)
            continue

        title_text = '{}: '.format(title)

        if len(selector_contents) < 4:
            size = len(selector_contents)
        else:
            size = 4

        params = dict(title=title_text,
                      value=[selector_contents[0]],
                      options=selector_contents,
                      size=size,
                      name=column_name)

        selector = MultiSelect(**params)
        selectors.append(selector)

    return selectors

def create_resolution_radios():
    div = Div(text="Chart Resolution: ")
    labels = ["Yearly", "Monthly", "Weekly", "Daily"]
    chart_res_radios = RadioButtonGroup(labels=labels, active=1)

    return chart_res_radios, div

def create_central_bus_toggle():
    label = "Ignore Central Business District"
    central_bus_toggle = Toggle(label=label, active=False)

    return central_bus_toggle

def create_chart_type_radios():
    div = Div(text="Chart type: ")
    labels = ['Non-Cumulative', 'Cumulative']
    chart_type_radios = RadioButtonGroup(labels=labels, active=0)

    return chart_type_radios, div

def create_chart_by_radios():
    div = Div(text="Chart Lines By: ")
    labels = [s['title'] for s in data_conf.multi_selectors]
    select_chart_by_radios = RadioButtonGroup(labels=labels, active=0)

    return select_chart_by_radios, div

def create_date_sliders():
    sliders = []

    start_date = date(*map(int, data_conf.start_date.split('-')))
    end_date = date(*map(int, data_conf.end_date.split('-')))

    params = dict(title="Date", name=data_conf.primary_date, start=start_date,
                  end=end_date, value=(start_date, end_date), step=1)

    slider = DateRangeSlider(**params)
    sliders.append(slider)

    return sliders

def create_hours_slider():
    params = dict(title="Hours", start=0, end=23, value=(0, 23))
    hours_selector = RangeSlider(**params)

    return hours_selector

    inputs = widgetbox(*controls, sizing_mode='fixed', width=420)

    return inputs

def create_widget_box(controls, sizing_mode='fixed', width=375):
    return widgetbox(*controls, sizing_mode=sizing_mode, width=width)

def create_layout(input_widgets, map_fig, chart_fig):
    col_1 = column(input_widgets)
    col_2 = column(map_fig, chart_fig)

    page_layout = layout([[col_1, col_2]], sizing_mode='fixed')

    return page_layout

def create_map_figure():
    palette = sns.cubehelix_palette(32, dark=.075, light=.92).as_hex()
    color_mapper = LogColorMapper(palette)

    #color_mapper.low = 0
    #color_mapper.high = 100

    def config_fig(fig):
        fig.title.text = proj_conf.project_title
        fig.add_tile(tileset)

        #disable irrelevent info
        fig.xaxis.visible = False
        fig.yaxis.visible = False
        fig.grid.grid_line_color = None
        fig.toolbar.logo = None
        fig.toolbar_location = None

        ##setup tools and setup wheel zoom as active
        wheel_zoom = WheelZoomTool()
        pan_tool = PanTool()
        fig.add_tools(wheel_zoom, pan_tool)
        fig.toolbar.active_scroll = wheel_zoom

        #create color bar and its pallette
        #ticker = LogTicker(desired_num_ticks=8)
#        color_mapper.low = 0
#        color_mapper.high = 1000

        color_bar = ColorBar(color_mapper=color_mapper, location=(0, 0), name='color_bar')
        fig.add_layout(color_bar, 'right')
        fig.right[0].formatter.use_scientific = False

        print(fig.__dict__)

        return fig

    def get_geodata_source():
        with open('data/default_geojson', 'r') as f:
            default_source = GeoJSONDataSource(geojson=f.read())

        return default_source

    def init_grid(fig, source):
        patches_opts = {
            'xs': 'xs',
            'ys': 'ys',
            'source': source,
            'fill_color': {'field': 'data_val', 'transform': color_mapper},
            'line_color': 'black',
            'line_width': .01,
            'fill_alpha': 0.8
        }
        fig.patches(**patches_opts)
        return fig

    geomap_opts = {
        'background_fill_color': None,
        'plot_width': 800,
        'tooltips': [("Count", "@data_val")],
        'tools': '',
        'x_axis_type': "mercator",
        'y_axis_type': "mercator",
        'x_range': (-9789724.66045665, -9742970.474323519),
        'y_range': (5107551.543942757, 5164699.247119262),
        'output_backend': 'webgl'
    }
    map_fig = figure(**geomap_opts)
    map_source = get_geodata_source()

    map_fig = init_grid(map_fig, map_source)
    map_fig = config_fig(map_fig)

    return map_fig, map_source

def create_chart_figure():
    def config_fig(fig):
        chart_title = Title()
        #chart_title.text = "Chart Title CHANGE ME"
        fig.title = chart_title

        chart_wheel_zoom = WheelZoomTool()
        chart_pan_tool = PanTool()

        fig.add_tools(chart_wheel_zoom, chart_pan_tool)
        fig.toolbar.active_scroll = chart_wheel_zoom

        fig.xaxis.visible = False
        chart_lines = None
        chart_hover = None

        fig.toolbar.logo = None
        fig.toolbar_location = None

        return fig

    chart_opts = {
        'plot_width':840,
        'plot_height':350,
        'tools':'',
        'x_axis_type': 'datetime',
        'output_backend': 'webgl',
        'title': '',
    }
    chart_fig = figure(**chart_opts)
    chart_fig = config_fig(chart_fig)

    return chart_fig

def update_chart(chart_data):
    x_count = len(chart_data['xs'])

    chart_palette = sns.hls_palette(x_count, l=.5, s=.6).as_hex()

    datetime_xs = []
    for raw_xs in chart_data['xs']:
        datetime_xs.append([datetime.strptime(x, '%Y-%m-%d 00:00:00') for x in raw_xs])

    chart_data['xs_datetime'] = datetime_xs
    chart_data['line_color'] = chart_palette

    old_multilines = [r for r in chart_fig.renderers if r.name == 'multi_lines']

    [chart_fig.renderers.remove(r) for r in old_multilines]


    params = {
        'xs': 'xs_datetime',
        'ys': 'ys',
        'line_width': 1.5,
        'line_alpha': .8,
        'line_color': 'line_color',
        'name': 'multi_lines',
        'source': ColumnDataSource(chart_data),
    }

    chart_fig.multi_line(**params)

    for tool in chart_fig.tools:
        if tool.name == 'chart_hover_tool':
            del tool

    chart_by_name = [s['title'] for s in data_conf.multi_selectors][chart_by_radios.active]
    chart_tooltips = [
        ("count", "$y{int}"),
        ("Date", "$x{%F}"),
        (chart_by_name, "@keys"),
     ]

    hovertool_opts = dict(
                       tooltips=chart_tooltips,
                       formatters={'$x': 'datetime'},
                       line_policy='nearest',
                       mode='mouse',
                       name='chart_hovertool')

    chart_hovertool = HoverTool(**hovertool_opts)
    chart_fig.add_tools(chart_hovertool)
    chart_fig.xaxis.visible = True
 

def get_widget_vals():
    def cleanup(val):
        val = ' '.join(str(val).split(' ')[:-1])
        return val
        
    ###
    date_slider_vals = []
    for slider in date_sliders:
        name = slider.name
        slider_start, slider_end = slider.value_as_datetime

        slider_start = datetime.strftime(slider_start, '%Y-%m-%d')
        slider_end = datetime.strftime(slider_end, '%Y-%m-%d')

        date_slider_vals.append((slider.name, (slider_start, slider_end)))

    date_slider_vals = dict(date_slider_vals)

    ###
    project_selector_vals = {}
    for selector in project_selectors:
        name = selector.name
        if name not in project_selector_vals:
            project_selector_vals[name] = []

        for selected in selector.value:
            project_selector_vals[name].append(cleanup(selected))

    ###
    vals = dict( 
        is_business_district = busdist_toggle.active,
        resolution_idx = resolution_radios.active,
        chart_by_idx = chart_by_radios.active,
        chart_type = chart_type_radios.active,
        date_sliders = date_slider_vals,
        date_by = date_by_radios.active,
        map_by = map_by_radios.active,
        project_selectors = project_selector_vals
    )

    return vals

def get_new_data():
    widget_vals = json.dumps(get_widget_vals())

    resp = requests.get('http://localhost:5000/', json=widget_vals).json()
    ret = json.loads(resp)

    print(widget_vals)

    return ret

def update_map(new_geojson):
    map_source.geojson = json.dumps(new_geojson)

    print(str(new_geojson)[:500])
    color_bar = [r for r in map_fig.right if r.name == 'color_bar'][0]

    map_vals = sorted([feat['properties']['data_val'] for feat in new_geojson['features']])

    max_val = map_vals[-1]
    std_dev = np.std(map_vals)
    high = std_dev * 6

    color_bar.color_mapper.low = np.median(map_vals)
    if high == 0:
        high = 1

    color_bar.color_mapper.high = high
  
    for tool in map_fig.tools:
        print('before dict ', tool.__dict__)
        if tool.name == 'map_hover_tool':
            tool.tooltips = [('', '')]

    map_by_name = [s['name'] for s in data_conf.geo_files][map_by_radios.active]
    chart_by_name = [s['title'] for s in data_conf.multi_selectors][chart_by_radios.active]

    map_tooltips = [('Search Warrants', "@data_val"), (map_by_name, "@id")]

    hovertool_opts = dict(
                       tooltips=map_tooltips,
                       name='map_hover_tool')

    map_hovertool = HoverTool(**hovertool_opts)
    map_fig.add_tools(map_hovertool)

    #TODO: yo dawg use some actual stats
    #val_count = len(map_vals)
    #if val_count >= 10:
    #    color_bar.color_mapper.high = int(map_vals[-2])
    #elif val_count == 1:
    #    color_bar.color_mapper.high = int(map_vals[-1])

def do_update():
    print("Updating...")
    update_button.label = "Updating..."
    update_button.disabled = True

    print("Getting data")
    new_data = get_new_data()

    update_map(new_data['geojson'])
    
    print("Updating chart..")
    chart_data = new_data['chart_xys']
    update_chart(chart_data)

    print("Done updating.")
    update_button.label = "Update"
    update_button.disabled = False

    return

def create_date_radios():
    select_rbg_div = Div(text="Based on Event: ")

    def clean_label(label):
        ret = label.replace('_', ' ').title()
        return ret

    labels = [clean_label(l) for l in data_conf.date_fields]
    date_by_button_group = RadioButtonGroup(labels=labels, active=0)

    return date_by_button_group, select_rbg_div

def create_map_by_radios():
    select_mb_div = Div(text="Map Boundaries: ")

    labels = [g['name'] for g in data_conf.geo_files]
    map_by_button_group = RadioButtonGroup(labels=labels, active=0)

    return map_by_button_group, select_mb_div

map_fig, map_source = create_map_figure()
chart_fig = create_chart_figure()
chart_lines = None

busdist_toggle = create_central_bus_toggle()
resolution_radios, rr_div = create_resolution_radios()

#display_by_radios, dbr_div = create_display_by_radios()

chart_type_radios, ctr_div = create_chart_type_radios()
chart_by_radios, cb_div = create_chart_by_radios()
date_by_radios, dr_div = create_date_radios()
map_by_radios, mb_div = create_map_by_radios()

project_selectors = create_project_selectors()

date_sliders = create_date_sliders()

update_button = create_update_button(do_update)

controls = [
    *date_sliders,
    *project_selectors,
    #*(dr_div, date_by_radios),
    *(ctr_div, chart_type_radios),
    *(cb_div, chart_by_radios),
    *(rr_div, resolution_radios),
    *(mb_div, map_by_radios),
    busdist_toggle,
    update_button
]

#ignoring uninitialized controls
while None in controls:
    controls.remove(None)

input_widgets = create_widget_box(controls)
layout = create_layout(input_widgets, map_fig, chart_fig)

do_update()

curdoc().add_root(layout)

from datetime import datetime
from fileinput import filename
import os
from google.cloud import storage
from flask import Flask, request,  render_template
import json
import requests
import glob
import pandas as pd
import folium
from folium import FeatureGroup, LayerControl
from folium import raster_layers

app = Flask(__name__)

bucket_name = 'datasci-kalampree'
img_dest_local = 'tmp/'


@app.route('/post', methods=['POST'])
def post_img_dir():
    if request.method == 'POST':
        json_data = request.json
        print(json_data)
        print(type(json_data))
        get_img(json_data)
        gen_map(json_data)
        return "success"


def get_img(js):
    dir = js["image_path"]
    folder_path = 'https://storage.googleapis.com/datasci-kalampree/' 
    img_order = 0
    for image_name in dir:
        full_path = folder_path+image_name
        r = requests.get(full_path, allow_redirects=True)
        open(img_dest_local+'map_overlay_'+str(img_order)+'.png', 'wb').write(r.content)
        img_order=img_order+1


def gen_map(js):
    rain_data = js["data"]
    df = pd.DataFrame(rain_data, columns=['timestamp', 'province', 'lat', 'lon', 'rain_rate'])
    df['lat'] = df['lat'].astype(str)
    df['lon'] = df['lon'].astype(str)
    df['rain_rate'] = df['rain_rate'].astype(float)

    center = [13.83488825823995, 100.8463897705078]

    m = folium.Map(location=center, control_scale=True, zoom_start=9)

    folium.TileLayer('openstreetmap').add_to(m)
    folium.TileLayer('Stamen Terrain').add_to(m)
    folium.TileLayer('Stamen Toner').add_to(m)
    folium.TileLayer('Stamen Water Color').add_to(m)
    folium.TileLayer('cartodbpositron').add_to(m)
    folium.TileLayer('cartodbdark_matter').add_to(m)

    IMAGE_LIST = ['map_overlay_0.png','map_overlay_1.png','map_overlay_2.png','map_overlay_3.png','map_overlay_4.png']
    img_count = 0
    show_bool = True
    for x in df['timestamp'].sort_values().unique():
        feature_group = FeatureGroup(name=x, overlay=True, show=show_bool, control=True)
        # Marker(Pop-ups)
        data = df.loc[df['timestamp'] == x, ['province', 'lat', 'lon', 'rain_rate']].values.tolist()
        for i in range(len(data)):
            provice_mark, lat_mark, lon_mark, rain_mark = data[i]
            html = "<h2 style='font-family:verdana'><b> &nbsp" + provice_mark.upper() + "</h2></b>" + "<b style='font-family:verdana'>Lat: &nbsp: </b>"+str(lat_mark) + \
                "<br>" + "<b style='font-family:verdana'>Lon: </b>" + \
                str(lon_mark) + "<br>"+"<b style='font-family:verdana'>Rain rate: </b>" + \
                str(round(rain_mark, 2))
            iframe = folium.IFrame(html)
            popup = folium.Popup(iframe,
                             min_width=400,
                             max_width=600
                             )
            folium.Marker(
                location=[lat_mark, lon_mark],
                popup=popup,
                tooltip='Click here to see infomation'
            ).add_to(feature_group)

        # Image(Overlay)
        image_dir = img_dest_local+IMAGE_LIST[img_count]
        raster_layers.ImageOverlay(
            name=IMAGE_LIST[img_count].replace(".png", ""),
            image=image_dir,
            bounds=[[12.75026910981639, 99.73642387358632],
                    [14.91950740666351, 101.9563556674292]],
            opacity=0.7,
            interactive=True,
            cross_origin=False,
            zindex=1
        ).add_to(feature_group)
        feature_group.add_to(m)

            # increase temp
        img_count = img_count+1
        show_bool = False
        print(feature_group)
        print(x)

    LayerControl().add_to(m)
    m.save(os.path.join('.', 'templates/rain_rate_map.html'))
    return

@app.route('/map', methods=['GET'])
def render_map():
    if request.method == 'GET':
        return render_template('rain_rate_map.html')


if __name__ == '__main__':
    app.run(debug=False,host='0.0.0.0',port=8000)

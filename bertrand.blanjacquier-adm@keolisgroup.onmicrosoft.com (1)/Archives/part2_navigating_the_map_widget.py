# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 - Navigating the map widget

# COMMAND ----------

# MAGIC %md
# MAGIC <h1>Table of Contents<span class="tocSkip"></span></h1>
# MAGIC <div class="toc"><ul class="toc-item"><li><span><a href="#Using-the-map-widget" data-toc-modified-id="Using-the-map-widget-1">Using the map widget</a></span><ul class="toc-item"><li><span><a href="#Buttons" data-toc-modified-id="Buttons-1.1">Buttons</a></span><ul class="toc-item"><li><span><a href="#1.-Zoom-in" data-toc-modified-id="1.-Zoom-in-1.1.1">1. Zoom in</a></span></li><li><span><a href="#2.-Zoom-out" data-toc-modified-id="2.-Zoom-out-1.1.2">2. Zoom out</a></span></li><li><span><a href="#3.-Reset-the-compass-orientation" data-toc-modified-id="3.-Reset-the-compass-orientation-1.1.3">3. Reset the compass orientation</a></span></li><li><span><a href="#4.-2D-Map-to-3D-Scene" data-toc-modified-id="4.-2D-Map-to-3D-Scene-1.1.4">4. 2D Map to 3D Scene</a></span></li></ul></li></ul></li><li><span><a href="#Properties-of-the-map-widget" data-toc-modified-id="Properties-of-the-map-widget-2">Properties of the map widget</a></span><ul class="toc-item"><li><span><a href="#Operations-of-the-map-widget" data-toc-modified-id="Operations-of-the-map-widget-2.1">Operations of the map widget</a></span><ul class="toc-item"><li><span><a href="#Zoom-Level-and-Rotation" data-toc-modified-id="Zoom-Level-and-Rotation-2.1.1">Zoom Level and Rotation</a></span></li><li><span><a href="#Map-Center" data-toc-modified-id="Map-Center-2.1.2">Map Center</a></span></li><li><span><a href="#Extent" data-toc-modified-id="Extent-2.1.3">Extent</a></span></li><li><span><a href="#Basemap" data-toc-modified-id="Basemap-2.1.4">Basemap</a></span></li><li><span><a href="#Mode" data-toc-modified-id="Mode-2.1.5">Mode</a></span></li><li><span><a href="#Heading,-tilt-and-scale" data-toc-modified-id="Heading,-tilt-and-scale-2.1.6">Heading, tilt and scale</a></span></li></ul></li></ul></li><li><span><a href="#Using-multiple-map-widgets" data-toc-modified-id="Using-multiple-map-widgets-3">Using multiple map widgets</a></span><ul class="toc-item"><li><span><a href="#Demo:-creating-multiple-widgets-in-the-same-notebook" data-toc-modified-id="Demo:-creating-multiple-widgets-in-the-same-notebook-3.1">Demo: creating multiple widgets in the same notebook</a></span><ul class="toc-item"><li><span><a href="#Stacking-maps-using-HBox-and-VBox" data-toc-modified-id="Stacking-maps-using-HBox-and-VBox-3.1.1">Stacking maps using HBox and VBox</a></span></li><li><span><a href="#Synchronizing-nav-between-multiple-widgets" data-toc-modified-id="Synchronizing-nav-between-multiple-widgets-3.1.2">Synchronizing nav between multiple widgets</a></span></li><li><span><a href="#The-synced-display-of-4-maps" data-toc-modified-id="The-synced-display-of-4-maps-3.1.3">The synced display of 4 maps</a></span></li></ul></li></ul></li><li><span><a href="#Conclusion" data-toc-modified-id="Conclusion-4">Conclusion</a></span></li></ul></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using the map widget
# MAGIC 
# MAGIC The `GIS` object includes a map widget for displaying geographic locations, visualizing GIS content, and displaying the results of your analysis. To use the map widget, call `gis.map()` and assign it to a variable that you will then be able to query to bring up the widget in the notebook:

# COMMAND ----------

import arcgis
from arcgis.gis import GIS
# Create a GIS object, as an anonymous user for this example
gis = GIS()

# COMMAND ----------

# Create a map widget
map1 = gis.map('Redlands, CA') # Passing a place name to the constructor
                               # will initialize the extent of the map.
map1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Buttons
# MAGIC 
# MAGIC Now that we have created a map view, let us explore the default buttons enabled on the widget:
# MAGIC 
# MAGIC #### 1. Zoom in
# MAGIC 
# MAGIC Click the "+" sign shown on the top left corner of the widget (marked as button #1 in previous map output) to zoom into details of the map. Users can either manually zoom into a desired level of detail or set the zoom levels to an assigned number, which we will elaborate on in the next section.
# MAGIC 
# MAGIC #### 2. Zoom out
# MAGIC 
# MAGIC Click the "-" sign shown on the top left corner of the widget (marked as button #2 in previous map output) to zoom out to a rough display of the map. Users can either manually zoom out to a desired level of details, or set the zoom levels to an assigned number, which we will elaborate on in the next section.
# MAGIC 
# MAGIC #### 3. Reset the compass orientation
# MAGIC 
# MAGIC Click the compass sign (marked as button #3 in the previous map display) to switch the map's heading to 0.0 (north) in relation to the current device, and click again to switch back to the absolute 0.0 north.
# MAGIC 
# MAGIC #### 4. 2D Map to 3D Scene
# MAGIC 
# MAGIC Click the "Map to Scene" button (marked as #4 in the previous map display) to switch the current view from a 2D Map to a 3D Scene. Click the button again to switch back.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Properties of the map widget

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operations of the map widget
# MAGIC 
# MAGIC The map widget has several properties that you can query and set, such as its zoom level, basemap, height, extent, mode, heading, rotation, tilt, scale, etc.
# MAGIC 
# MAGIC #### Zoom Level and Rotation

# COMMAND ----------

# Create a map widget
map2 = gis.map('Redlands, CA') # Passing a place name to the constructor
                               # will initialize the extent of the map.
map2

# COMMAND ----------

map2.zoom

# COMMAND ----------

# MAGIC %md
# MAGIC Assigning a value to the zoom property will update the widget, which is equivalent to manually clicking the "zoom in" button twice.

# COMMAND ----------

map2.zoom = 9

# COMMAND ----------

# MAGIC %md
# MAGIC You can also set the rotation property for the 2D mode. This can similarly be achieved by right-clicking and dragging on the map.

# COMMAND ----------

map2.rotation = 45

# COMMAND ----------

# MAGIC %md
# MAGIC Your notebook can have as many of these widgets as you wish. Let's create another map widget and modify some of its properties.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Map Center
# MAGIC 
# MAGIC The center property reveals the coordinates of the center of the map.

# COMMAND ----------

map3 = gis.map() # creating a map object with default parameters
map3

# COMMAND ----------

map3.center

# COMMAND ----------

# MAGIC %md
# MAGIC If you know the latitude and longitude of your place of interest, you can assign it to the center property. For instance, we can now set the center to be within California.

# COMMAND ----------

map3.center = {'spatialReference': {'latestWkid': 3857, 'wkid': 102100},
               'x': -13044706.248636946,
               'y': 4036244.856763349}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extent
# MAGIC 
# MAGIC You can use `geocoding` to get the coordinates of different places and use those coordinates to drive the extent property. `Geocoding` converts place names to coordinates and can be implemented by using the `arcgis.geocoding.geocode()` function.
# MAGIC 
# MAGIC Let's geocode "Disneyland, CA" and set the map's extent to the geocoded location's extent:

# COMMAND ----------

location = arcgis.geocoding.geocode('Disneyland, CA', max_locations=1)[0]
map3.extent = location['extent']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Basemap
# MAGIC 
# MAGIC <a href="https://livingatlas.arcgis.com/en/">ArcGIS Living Atlas of the World</a> is an evolving collection of authoritative, curated, ready-to-use global geographic information from Esri and the GIS user community. One of the most used types of content from the Living Atlas is basemaps. Basemaps are layers on your map over which all other operational layers that you add are displayed. Basemaps typically span the full extent of the world and provide context to your `GIS` layers. It helps viewers understand where each feature is located as they pan and zoom to various extents.
# MAGIC 
# MAGIC When you create a new map or scene, you can choose which basemap you want from the basemap gallery in the Map Viewer. By default, the basemap gallery for your organization is a pre-configured collection from Esri using the Living Atlas. There are many more Living Atlas basemaps to choose from, and you can create your own custom basemap gallery with the ones you like. Learn more on this <a href="https://www.esri.com/arcgis-blog/products/arcgis-online/mapping/living-atlas-custom-basemap-gallery/">here</a>.
# MAGIC 
# MAGIC As an administrator of your organization, you can change which basemaps your organization uses by creating a custom basemap gallery. The custom gallery can include a combination of your own basemaps, plus Living Atlas basemaps. In a nutshell, the steps to create a custom basemap gallery are as follows: 
# MAGIC  - Createa group for your custom basemap gallery.
# MAGIC  - Add maps you want to use as basemaps to the group.
# MAGIC  - Set the group as your organization’s basemap gallery.
# MAGIC  
# MAGIC These steps are detailed in the <a href="https://www.esri.com/arcgis-blog/products/arcgis-online/mapping/custom-basemap-gallery/">Create a custom basemap gallery for your organization</a> blog. After step one is done, users can move forward to :
# MAGIC  - <a href="https://doc.arcgis.com/en/arcgis-online/share-maps/create-groups.htm">create a custom basemap group</a>
# MAGIC  - <a href="https://www.esri.com/arcgis-blog/products/arcgis-online/mapping/living-atlas-custom-basemap-gallery/">adding maps from the Living Atlas to populate your custom gallery</a>
# MAGIC 
# MAGIC When your `gis` connection is created anonymously, the `basemaps` and `gallery_basemaps` properties of the created `MapView` object displays the default themes. While signing onto your own organization, the two properties would display your own customized options if set.
# MAGIC 
# MAGIC Your map can have a number of different basemaps. To see what basemaps are included with the widget, query the `basemaps` property:

# COMMAND ----------

map3.basemap # the current basemap being used

# COMMAND ----------

map3.basemaps # the basemap galleries

# COMMAND ----------

# MAGIC %md
# MAGIC You can assign any one of the supported basemaps to the `basemap` property to change the basemap. For instance, you can change the basemap to the `satellite` basemap as below:

# COMMAND ----------

map3.basemap = 'satellite'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mode
# MAGIC 
# MAGIC The map widget also includes support for a 3D mode! You can specify the `mode` parameter either through `gis.map(mode="foo")` or by setting the mode property of any initiated map object. Run the following cell:

# COMMAND ----------

usa_map = gis.map('USA', zoomlevel=4, mode="3D") # Notice `mode="3D"`
usa_map

# COMMAND ----------

# And you can set the mode separately
usa_map.mode = "2D"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Heading, tilt and scale
# MAGIC 
# MAGIC Just like the 2D mode, you can pan by clicking-and-dragging with the left mouse button, and you can zoom with the mouse wheel. In 3D mode, clicking-and-dragging with the right mouse button modifies the tilt field and the heading field.
# MAGIC 
# MAGIC `tilt` is a number from 0-90, with 0 representing a top-down 'birds-eye' view, while 90 represents being completely parallel to the ground, facing the horizon.

# COMMAND ----------

usa_map.tilt

# COMMAND ----------

usa_map.tilt = 22.63

# COMMAND ----------

# MAGIC %md
# MAGIC It's important to note that 2D mode uses `rotation` to specify the number of angles clockwise from due north, while 3D mode uses `heading` to specify the number of degrees counterclockwise of due north. See the API reference for more information.

# COMMAND ----------

usa_map.heading

# COMMAND ----------

usa_map.heading = 60

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Using multiple map widgets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo: creating multiple widgets in the same notebook
# MAGIC 
# MAGIC #### Stacking maps using HBox and VBox
# MAGIC 
# MAGIC One commonly adopted workflow for creating multiple widgets in the same notebook is to embed Python API map widgets within `HBox` and `VBox`. First, let's walk through an example of displaying Landsat imagery of two different dates side by side using an `HBox` structure:

# COMMAND ----------

# search for the landsat multispectral imagery layer
landsat_item = gis.content.search("Landsat Multispectral tags:'Landsat on AWS','landsat 8', 'Multispectral', 'Multitemporal', 'imagery', 'temporal', 'MS'", 'Imagery Layer', outside_org=True)[0]
landsat = landsat_item.layers[0]
landsat

# COMMAND ----------

import pandas as pd
from datetime import datetime
from ipywidgets import *
from arcgis import geocode

# COMMAND ----------

aoi = {'xmin': -117.58051663099998,
       'ymin': 33.43943880400006,
       'xmax': -114.77651663099998,
       'ymax': 36.243438804000064,
       'spatialReference': {'latestWkid': 4326, 'wkid': 102100},}

# COMMAND ----------

selected1 = landsat.filter_by(where="(Category = 1) AND (CloudCover <=0.10)", 
                              time=[datetime(2017, 11, 15), datetime(2018, 1, 1)],
                              geometry=arcgis.geometry.filters.intersects(aoi))

df = selected1.query(out_fields="AcquisitionDate, GroupName, CloudCover, DayOfYear", 
                     order_by_fields="AcquisitionDate").sdf
df['AcquisitionDate'] = pd.to_datetime(df['AcquisitionDate'], unit='ms')
df.tail(5)

# COMMAND ----------

selected2 = landsat.filter_by(where="(Category = 1) AND (CloudCover <=0.10)", 
                              time=[datetime(2021, 11, 15), datetime(2022, 1, 1)],
                              geometry=arcgis.geometry.filters.intersects(aoi))

df = selected2.query(out_fields="AcquisitionDate, GroupName, CloudCover, DayOfYear", 
                     order_by_fields="AcquisitionDate").sdf
df['AcquisitionDate'] = pd.to_datetime(df['AcquisitionDate'], unit='ms')
df.tail(5)

# COMMAND ----------

def side_by_side(address, layer1, layer2):
    location = geocode(address)[0]

    satmap1 = gis.map(location)
    satmap1.add_layer(layer1)

    satmap2 = gis.map(location)
    satmap2.add_layer(layer2)

    satmap1.layout=Layout(flex='1 1', padding='6px', height='450px')
    satmap2.layout=Layout(flex='1 1', padding='6px', height='450px')

    box = HBox([satmap1, satmap2])
    return box

# COMMAND ----------

side_by_side("San Bernadino County, CA", selected1, selected2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Synchronizing nav between multiple widgets
# MAGIC 
# MAGIC A side-by-side display of two maps is great for users wanting to explore the differences between two maps. However, if one map gets dragged or panned, the other map is not following the movements automatically. The methods `sync_navigation` and `unsync_navigation` can be introduced to resolve this. With these methods, we can modify the previous example to have the maps in sync.

# COMMAND ----------

def side_by_side2(address, layer1, label1, layer2, label2):
    location = geocode(address)[0]

    satmap1 = gis.map(location)
    satmap1.add_layer(layer1)

    satmap2 = gis.map(location)
    satmap2.add_layer(layer2)
 
    # create 2 hbox - one for title, another for maps
    hb1 = HBox([Label(label1), Label(label2)])
    hb2 = HBox([satmap1, satmap2])
    
    # set hbox layout preferences
    hbox_layout = Layout()
    hbox_layout.justify_content = 'space-around'
    hb1.layout, hb2.layout = hbox_layout, hbox_layout
    
    # sync all maps
    satmap1.sync_navigation(satmap2)

    return VBox([hb1,hb2])

# COMMAND ----------

side_by_side2("San Bernadino County, CA", selected1, 'Dec 2017', selected2, 'Dec 2021')

# COMMAND ----------

# MAGIC %md
# MAGIC #### The synced display of 4 maps
# MAGIC 
# MAGIC Now let's sync the display of 4 maps:

# COMMAND ----------

selected1r = landsat.filter_by(where="(Category = 1) AND (CloudCover <=0.10)", 
                               time=[datetime(2018, 11, 15), datetime(2019, 1, 1)],
                               geometry=arcgis.geometry.filters.intersects(aoi))

df = selected1r.query(out_fields="AcquisitionDate, GroupName, CloudCover, DayOfYear", 
                     order_by_fields="AcquisitionDate").sdf
df['AcquisitionDate'] = pd.to_datetime(df['AcquisitionDate'], unit='ms')
df.tail(5)

# COMMAND ----------

selected2l = landsat.filter_by(where="(Category = 1) AND (CloudCover <=0.10)", 
                              time=[datetime(2020, 11, 15), datetime(2021, 1, 1)],
                              geometry=arcgis.geometry.filters.intersects(aoi))

df = selected2l.query(out_fields="AcquisitionDate, GroupName, CloudCover, DayOfYear", 
                     order_by_fields="AcquisitionDate").sdf
df['AcquisitionDate'] = pd.to_datetime(df['AcquisitionDate'], unit='ms')
df.tail(5)

# COMMAND ----------

def side_by_side3(address, layers_list, labels_list):
    [layer1, layer2, layer3, layer4] = layers_list
    [label1, label2, label3, label4] = labels_list
    location = geocode(address)[0]

    satmap1 = gis.map(location)
    satmap1.add_layer(layer1)

    satmap2 = gis.map(location)
    satmap2.add_layer(layer2)
    
    satmap3 = gis.map(location)
    satmap3.add_layer(layer3)

    satmap4 = gis.map(location)
    satmap4.add_layer(layer4)
 
    # create 2 hbox - one for title, another for maps
    hb1 = HBox([Label(label1), Label(label2)])
    hb2 = HBox([satmap1, satmap2])
    hb3 = HBox([Label(label3), Label(label4)])
    hb4 = HBox([satmap3, satmap4])
    
    # set hbox layout preferences
    hbox_layout = Layout()
    hbox_layout.justify_content = 'space-around'
    hb1.layout, hb2.layout, hb3.layout, hb4.layout = hbox_layout, hbox_layout, hbox_layout, hbox_layout
    
    # sync all maps
    satmap1.sync_navigation(satmap2)
    satmap1.sync_navigation(satmap3)
    satmap1.sync_navigation(satmap4)

    return VBox([hb1,hb2,hb3,hb4])

# COMMAND ----------

side_by_side3("San Bernadino County, CA", 
              [selected1,selected1r,selected2l,selected2], 
              ['Dec 2017', 'Dec 2018', 'Dec 2020', 'Dec 2021'])

# COMMAND ----------

# MAGIC %md
# MAGIC Dragging and panning onto any of these 4 maps will lead to synchronous movements of the other three maps, which means that the map center, zoom levels, and extent of these maps will always stay the same. This is one of the biggest advantages of `sync_navigation()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC 
# MAGIC In Part 2 of the guide series, we have talked about the use of map widgets, including buttons, features, and properties, and have seen three examples of displaying multiple map widgets in a group view using `HBox` and `VBox`. In the next chapter, we will discuss how to visualize spatial data on the map widget.
# MAGIC 
# MAGIC <a href="#Part-2---Navigating-the-map-widget">Back to Top</a>

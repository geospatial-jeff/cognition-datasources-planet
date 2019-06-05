from datasources import Manifest

def PlanetData(event, context):
    manifest = Manifest()
    manifest['PlanetData'].search(**event)
    response = manifest.execute()
    return response



from data_pipeline.WebScrape import WebScrape

if __name__ == '__main__':

    objeto = WebScrape('/config/config.yaml')
    objeto.get_links()
    objeto.transform()

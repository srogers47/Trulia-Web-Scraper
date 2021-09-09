#!/usr/bin/env python3 

import asyncio 
import aiohttp
import pymongo
import lxml 
import gzip
from bs4 import BeautifulSoup as BS 
import unittest
import os  



class WebScraper:
    """
    """
    base_url = "https://www.trulia.com/" # Gather grab cookies and headers for requests 
    sitemap  = "https://www.trulia.com/sitemaps/xml/p/index.xml" #XML list of gz compressed urls for all properties.

    async def fetch_urls(self, session, gzip_url) -> list:
        #TODO
        """
        Read files containing urls.
        Fetch listings. 
        """
        async def prep_fetch() -> list:
            """ 
            Get urls to listings. 
            Stream gzip compressed files and write to local dir. 
            """
            async with session.get(gzip_url,) as resp: 
                chunk_size = 10 # Set chunk for streaming  
                # Name files based on url basename
                filename = "./urls/" + str(os.path.basename(gzip_url).strip(".gz")) 
                print(filename) 
                with open( filename, 'wb') as fd: # Save in urls dir 
                    while True:
                        chunk = await resp.content.read(chunk_size)
                        if not chunk:
                            break
                        fd.write(chunk)
                    print(f"File {filename} has been saved to urls/") 
        await prep_fetch() 


    async def dispatch(self, loop) -> dict:
        """
        Init ClientSession().
        Dispatch urls to unzip/decompress.
        Return dict of all datapoints including status of db insert:Bool. 
        """
        headers = [] # Empty array place-holder 
        urls = [] 
        async def get_headers(session) -> dict: #Expect CIMultiDict containing headers
            """Get headers from base site."""
            async with session.get(self.base_url) as resp: # Get headers from response   
                nonlocal headers # Bind headers to nearest non-global headers array
                headers = resp.headers # Get headers from response 
                assert resp.status == 200 # Test
                


        async def parse_xml(xml_content) -> list:
            """Return a list of gzipped urls from sitemaps""" 
            soup = BS(xml_content, "lxml") # Pass xml content and parser into soup
            nonlocal urls 
            for url in soup.find_all('loc'):
                urls.append(url.get_text()) 
            assert len(urls) > 10 
            return urls

        # Wrap enclosed methods in ClientSession() context manager.
        async with aiohttp.ClientSession(loop=loop) as session: 
            await get_headers(session) #Get headers 
            print(f"Headers: {headers}") 

            async with session.get(self.sitemap,) as resp:
                xml_content = await resp.text()# Get xml page containg urls 
            await parse_xml(xml_content)   # First layer of urls to sitemaps 
            print(f"Collected {len(urls)} urls.  \n {urls}") 
            assert len(urls) > 10 
           
           #Read gziped urls 
            tasks = [self.fetch_urls(session, gzip_url) for gzip_url in urls] 
            results = asyncio.gather(*tasks) 
            await results


if __name__=="__main__": 
    main = WebScraper() 
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(main.dispatch(loop)) 
    

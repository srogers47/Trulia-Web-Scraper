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
    urls = [] #Empty placeholder for urls 

    async def fetch_urls(self, session, gzip_url) -> list:
        #TODO
        """
        Fetch listings. 
        """
        async def load_in_urls(filename) -> dict: 
            """ 
            Read streamed gzip files from local dir.
            Cannot be done on the fly.
            Yield a dict {filename: lines}
            """
            with gzip.open(filename, 'rb') as f:
                xml_content = f.readlines()
                await self.parse_xml(str(xml_content)) 
                print(f"{len(self.urls)} urls extracted. \n {self.urls}")
                
        async def prep_fetch() -> str:
            """ 
            Get urls to listings. 
            Stream gzip compressed files and write to local dir. 
            """
            async with session.get(gzip_url) as resp: 
                chunk_size = 10 # Set chunk for streaming  
                # Name files based on url basename
                filename = "./urls/" + str(os.path.basename(gzip_url)) # .strip(".gz")) 
                print(filename) # Debug
                with open(filename, 'wb') as fd: # Save in urls dir 
                    while True: #Stream to files 
                        chunk = await resp.content.read(chunk_size)
                        if not chunk:
                            break
                        fd.write(chunk) # Write
                print(f"File {filename} has been saved to urls/") 
                await load_in_urls(filename) # Call helper to extract/load urls 

        await prep_fetch() #async gen func

        ### FETCH URLS 
        

    async def parse_xml(self, xml_content) -> list:
        """Return a list of urls from sitemaps' xml content"""
        urls = [] #Temp storage in mem 
        soup = BS(xml_content, "lxml") # Pass xml content and parser into soup
        for url in soup.find_all('loc'):
            urls.append(url.get_text()) 
        self.urls = urls 
        assert len(self.urls) > 10 
        

    async def dispatch(self, loop) -> dict:
        """
        Init ClientSession().
        Dispatch urls to unzip/decompress.
        Return dict of all datapoints including status of db insert:Bool. 
        """
        headers = [] # Empty array place-holder 
        async def get_headers(session) -> dict: #Expect CIMultiDict containing headers
            """Get headers from base site."""
            async with session.get(self.base_url) as resp: # Get headers from response   
                nonlocal headers # Bind headers to nearest non-global headers array
                headers = resp.headers # Get headers from response 
                assert resp.status == 200 # Test


        # Wrap methods in ClientSession() context manager.
        async with aiohttp.ClientSession(loop=loop) as session: 
            await get_headers(session) #Get headers 
            print(f"Headers: {headers}") 
            
            async with session.get(self.sitemap,) as resp:
                xml_content = await resp.text()# Get xml page containg urls to other sitemaps
                await self.parse_xml(xml_content)
                print(f"Collected {len(self.urls)} urls.  \n {self.urls}") 
                assert len(self.urls) > 10 
           
           #Read gziped urls 
            tasks = [self.fetch_urls(session, gzip_url) for gzip_url in self.urls] 
            results = asyncio.gather(*tasks) 
            await results


if __name__=="__main__": 
    main = WebScraper() 
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(main.dispatch(loop)) 
    

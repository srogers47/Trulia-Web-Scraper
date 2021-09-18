#!/usr/bin/env python3

import aiohttp
from lxml import html
from bs4 import BeautifulSoup as BS
import re

from seleniumwire import webdriver
from selenium.webdriver.firefox.options import Options

from PIL import Image
from pymongo import MongoClient
import numpy as np
from numpy import asarray
import blosc

import asyncio
import gzip
import os
from random import randint
from subprocess import call


class WebScraper:
    """
    Note: Test build.

    """
    base_url = "https://www.trulia.com/" # Gather grab cookies and headers for requests
    sitemap  = "https://www.trulia.com/sitemaps/xml/p/index.xml" #XML list of gz compressed urls for all properties.
    urls = [] #Empty placeholder for urls
    sleep_time = randint(1,5) # Randomized sleep/wait. Increase range for slower but sneakier crawl.
    proxy_wait_time = randint(2,3) # Give expressvpn a second to connect to a different relay.

    async def fetch_urls(self, session, gzip_url) -> list:
        """
        Fetch listings.
        Uses nested functions as there is room to implement more control flows
        in pipeline.  For example extracting urls from rental-properties sitemap.
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

        await prep_fetch()



    async def extract_listing(self, session, listing_url) -> dict:
        """
        Extract data listings.
        """
        async def interceptor(driver, listing_url) -> list:
            """
            Intercepts and modifies requests on the fly.
            View network requests for api data and static imageset urls.
            Return a list of request urls' to thumbnail imagesets of listing.
            """
            # Concatenate this str to url to view modal lightbox triggering imageset of thumbnails to load (Fetched from graphql api).
            modal_box = "?mid=0#lil-mediaTab" # Replace '-mediaTab' for different requests/data ie '-crime' returns requests to api for crime stats/rate.
            async with driver.get(str(listing_url) + str(modal_box)) as response: # Load modal and imageset requests with 'response'
                _requests = await response.requests # List of requests. Parse for imagset urls.
                requests = re.find_all(_requests, ("/pictures/thumbs_5/zillowstatic"))) # Parse list for thumbnail image urls.
                return requests

        async def download_images(session, image_url):
            """
            Download images from trulia listings and insert into mongodb.
            Note that the images are compressed bianries.
            """
            async with session.get(image_request) as resp:
                # Output of resp image will be binary, No need to compress again.
                # Use blosc to put binary into array for MongoClient instert.
                image = await resp





        async def parse_html(html_content) -> dict:
            """
            Provided html, parse for data points.
            Need to use xpath as classnames are dynamically generated/change frequently.
            """
            # xpaths to data points. These will most likely change.
            temp_sale_tag = "/html/body/div[2]/div[2]/div/div[2]/div[1]/div/div/div[2]/div[1]/span"
            temp_address = "/html/body/div[2]/div[2]/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div/div[1]/div[1]/h1/span[1]"
            temp_state_zip = " /html/body/div[2]/div[2]/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div/div[1]/div[1]/h1/span[2]"
            temp_price = "/html/body/div[2]/div[2]/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div/div[2]/div/h3/div"
            temp_beds = "/html/body/div[2]/div[2]/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div/div[2]/div/h3/div"
            temp_baths = " /html/body/div[2]/div[2]/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div/div[1]/div[2]/div[1]/div/ul/li[2]/div/div"
            temp_sqft = "/html/body/div[2]/div[2]/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div/div[1]/div[2]/div[1]/div/ul/li[3]/div/div"
            temp_hoa_fee = "/html/body/div[2]/div[2]/div/div[2]/div[2]/div[4]/div[2]/div/div[4]/div/div/div[3]"
            temp_heating = "/html/body/div[2]/div[2]/div/div[2]/div[2]/div[4]/div[2]/div/div[5]/div/div/div[3]"
            temp_cooling = "/html/body/div[2]/div[2]/div/div[2]/div[2]/div[4]/div[2]/div/div[6]/div/div/div[3]"
            temp_description = "/html/body/div[2]/div[2]/div/div[2]/div[2]/div[1]/div[1]/div[4]/div[2]/div"

            # Use lxml to parse xpath from soup.
            tree = html.fromstring(html_content) # Create element tree.
            sale_tag = tree.xpath(temp_sale_tag.text)
            address = tree.xpath(temp_address.text)
            state_zip = tree.xpath(temp_state_zip)
            price = tree.xpath(temp_price.text)
            beds = tree.xpath(temp_beds.text)
            baths = tree.xpath(temp_baths.text)
            sqft = tree.xpath(temp_sqft.text)
            hoa_fee = tree.xpath(temp_hoa_fee.text)
            heating = tree.xpath(temp_heating.text)
            cooling = tree.xpath(temp_cooling.text)
            description = tree.xpath(temp_descrption.text)
            # Return a dict of data points
            return {"Sale Tag": sale_tag,
                    "Address": address,
                    "State Zip Code": state_zip,
                    "Price": price,
                    "Beds": beds,
                    "Baths": baths,
                    "Square Footage": sqft,
                    "HOA Fees": hoa_fee,
                    "Cooling": cooling,
                    "Description": description}

        # Extract Listings
        # Initiate webdriver.
        options = Options()
        options.headless = True
        driver = webdriver.Firefox(options=options,executable_path=r"../geckodriver") # Make sure the driver is an exe.

        # Get listing response
        async with session.get(listing_url) as resp:
            html_content = resp.text()
            await asyncio.sleep(self.sleep_time) # Sleep a few second between every request.  Be nice to the trulia graphql api backend!
            await html_content
            datapoints = await parse_html(html_content)

            # Get listing images
            imageset_requests = await interceptor(driver, listing_url) # Call interceptor. Aggregates all requests for property images.
            await html_content
            datapoints = await parse_html(html_content)

            # Get listing images
            imageset_requests = await interceptor(driver, listing_url) # Call interceptor. Aggregates all requests for property images.
            images_task = [download_images(session, image_url) for image_url in imageset_requests]
            get_images = await asyncio.gather(*images_task)




    async def parse_xml(self, xml_content) -> list:
        """Return a list of urls from sitemaps' xml content"""
        urls = [] #Temp storage in mem
        soup = BS(xml_content, "lxml") # Pass xml content and parser into soup
        for url in soup.find_all('loc'):
            urls.append(url.get_text())
        self.urls = urls
        assert len(self.urls) > 10 # We should have way more.  Better end to end testing will be implemented in the store_data.py module. '


    async def dispatch(self, loop) -> dict:
        """
        Init ClientSession().
        Dispatch urls to unzip/decompress.
        Return dict of all datapoints including status of db insert:Bool.
        """
        # headers
        headers = ({'User-Agent':
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',\
            'Accept-Language': 'en-US, en;q=0.5'})
       # headers = [] # Empty array place-holder
       # async def get_headers(session) -> dict: #Expect CIMultiDict containing headers
       #     """Get headers from base site."""
       #     async with session.get(self.base_url) as resp: # Get headers from response
       #         headers = resp.headers # Get headers from response
       #         assert resp.status == 200 # Test

        async def switch_proxy(random_alias):
            """
            For dev testing purposes. This function will be refactored as a script imported via docker-compose.
            Run vpn_routing.sh to interact with expressvpn CLI.
            All outbound requests will pause during this transition.
            """
            # Run the vpn_routing script pass in the alias for vpn relay
            call(["bash", "./vpn_routing.sh", f"{random_alias}"]) # TODO if error try without "bash" in call()
            asyncio.sleep(self.proxy_wait_time)


        # Wrap method calls in ClientSession() context manager.
        async with aiohttp.ClientSession(loop=loop) as session:

            # Get xml page containg urls to active property sitemaps
            async with session.get(self.sitemap,) as resp:
                xml_content = await resp.text()
                await self.parse_xml(xml_content)
                print(f"Collected {len(self.urls)} urls.  \n {self.urls}")
                assert len(self.urls) > 10

           # Fetch Listing sitemaps. Extract/Read gzip urls to file
            tasks = [self.fetch_urls(session, gzip_url) for gzip_url in self.urls]
            results = asyncio.gather(*tasks)
            await results

            # Fetch Extracted listing urls & parse html
            # Vpn aliases for relay/ip rotation.
            vpn_aliases = ["hk2", "usny","uswd", "usse", "usda2", "usda", "usla", "ussf",
                    "sgju","in","cato","camo","defr1","ukdo","uklo","nlam","nlam2",
                    "esba","mx","ch2","frpa1","itmi"]
            random_alias = vpn_aliases[randint(0, int(len(vpn_aliases)))] # Randomize vpn relay/ip rotation.
            tasks = [self.extract_listing(session, listing_url, random_alias) for listing_url in self.urls]
            results = asyncio.gather(*tasks)
            await results



import os
import time
import itertools
from typing import Optional
from pathlib import Path
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class SeleniumWebDriverContextManager:
    """
    Context manager for initializing and quitting the Selenium WebDriver.
    """

    def __init__(
        self,
        driver_path: Optional[str] = None,
        options: Optional[webdriver.ChromeOptions] = None,
    ):
        self.driver_path = driver_path or os.environ.get("CHROMEDRIVER_PATH")
        if not self.driver_path:
            raise ValueError(
                "ChromeDriver path must be provided or set via CHROMEDRIVER_PATH env var"
            )

        self.options = options
        self.driver: Optional[WebDriver] = None

    def __enter__(self) -> WebDriver:
        service = Service(executable_path=self.driver_path)
        self.driver = webdriver.Chrome(service=service, options=self.options)
        return self.driver

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if self.driver:
            self.driver.quit()
            self.driver = None
        return False


def run_table_interaction(driver: WebDriver, csv_dir: Path) -> None:
    """
    ### 2. Table Interaction ###
    Extract the summary table and persist it as CSV.
    """

    column_names = []  # List to store column names from the table
    columns = {}  # Dictionary to store column data

    try:
        # identify the list of table columns
        table_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "table"))
        )
        column_elements = table_element.find_elements(By.CLASS_NAME, "y-column")
        if not column_elements:
            raise RuntimeError("Unable to locate table columns; check report structure")

        # iterate through columns to extract column names and values
        for column in column_elements:
            header_element = column.find_element(By.ID, "header")
            header_cell_elements = header_element.find_element(
                By.CLASS_NAME, "cell-text"
            )
            column_names.append(header_cell_elements.text)
            column_element = column.find_element(By.ID, "cells1")
            value_elements = column_element.find_elements(By.CSS_SELECTOR, "text")

            # construct pandas series of columns of data
            values = []
            for value in value_elements:
                values.append(value.text)

            columns[header_cell_elements.text] = values

        # create dataframe and save it to .csv file
        csv_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(columns)
        df.to_csv(csv_dir / "table.csv", index=False)
        # print(df)
    except TimeoutException as exc:
        raise RuntimeError("Timed out waiting for the summary table to render") from exc
    except NoSuchElementException as exc:
        raise RuntimeError("Table layout changed; selectors no longer valid") from exc


def run_doughnut_chart_interaction(
    driver: WebDriver,
    csv_dir: Path,
    screenshot_dir: Path,
) -> None:
    """
    ### 3. Doughnut Chart Interaction ###
    Drive the doughnut chart filters, capture screenshots, and export CSV snapshots.
    """

    # Generate all possible toggle states for 3 doughnut chart filter toggles (False/True combinations)
    all_toggle_states = list(itertools.product([False, True], repeat=3))
    csv_columns: list[str] = ["Facility Type", "Min Average Time Spent"]

    try:
        chart_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "svg-container"))
        )

        # Scroll the chart element into view - otherwise screenshot will have white cutoff
        driver.execute_script(
            "arguments[0].scrollIntoView({behavior: 'instant', block: 'center', inline: 'center'});",
            chart_element,
        )

        time.sleep(5)
        # use traces within the legend to filter the doughnut chart
        element = driver.find_element(By.CLASS_NAME, "legend")
        traces_elements = element.find_elements(By.CLASS_NAME, "traces")
        if not traces_elements:
            raise RuntimeError("Doughnut chart legend found but contains no traces")
        time.sleep(5)

        screenshot_dir.mkdir(parents=True, exist_ok=True)
        csv_dir.mkdir(parents=True, exist_ok=True)

        # loop through possible toggle states and set doughnut chart filters accordingly
        for i, toggle_state in enumerate(all_toggle_states):
            rows = []
            for j, trace in enumerate(traces_elements):
                # if style attribute of a trace is equal to "opacity: 1;", then the trace is selected and slice is visible
                trace_state = trace.get_attribute("style") == "opacity: 1;"
                if trace_state != toggle_state[j]:
                    trace.click()
                    time.sleep(5)  # wait for hover popup to disappear

            chart_element.screenshot(str(screenshot_dir / f"screenshot{i}.png"))

            # collect data for .csv file with filtered doughnut chart data
            pielayer_element = driver.find_element(By.CLASS_NAME, "pielayer")
            slices = pielayer_element.find_elements(By.TAG_NAME, "text")

            for slice in slices:
                tspans = slice.find_elements(By.TAG_NAME, "tspan")
                rows.append(
                    {
                        "Facility Type": tspans[0].text,
                        "Min Average Time Spent": tspans[1].text,
                    }
                )

            # create dataframe and save it to .csv file
            df = pd.DataFrame(rows, columns=csv_columns)
            df.to_csv(csv_dir / f"doughnut{i}.csv", index=False)
            # print(df)
    except TimeoutException as exc:
        raise RuntimeError(
            "Timed out waiting for doughnut chart elements to load"
        ) from exc
    except NoSuchElementException as exc:
        raise RuntimeError(
            "Doughnut chart structure changed; selectors invalid"
        ) from exc


if __name__ == "__main__":
    driver_path = r"C:\\Users\\Vaidotas_Sruogis\\Documents\\CodeExperiments\\ChromeDriver\\chromedriver-win64\\chromedriver-win64\\chromedriver.exe"  # os.environ.get("CHROMEDRIVER_PATH")
    html_file = Path(r".\generated_report\report.html")

    csv_dir = Path("./csv")
    screenshot_dir = Path("./screenshots")

    with SeleniumWebDriverContextManager(driver_path=driver_path) as driver:
        url = html_file.resolve().as_uri()  # construct url to local report.html file
        driver.get(url)  # open it in the browser
        time.sleep(5)   # wait for the file to open

        # 2. Table Interactions
        run_table_interaction(driver, csv_dir)

        # 3. Doughnut Chart Interactions
        run_doughnut_chart_interaction(driver, csv_dir, screenshot_dir)

*** Settings ***
Library    SeleniumLibrary
Library    OperatingSystem
# Custom Libraries
Library    helper.HelperLibrary


*** Variables ***
${REPORT_FILE}             ${CURDIR}${/}report.html
${PARQUET_FOLDER}          ${CURDIR}${/}data${/}parquet_data${/}facility_type_avg_time_spent_per_visit_date
${PARQUET_COLUMNS}         facility_type, visit_date, avg_time_spent
@{KEY_COLUMNS}             facility_type    visit_date
${DATE_COLUMN}             visit_date
${FILTER_START_DATE}       2025-11-15
${FILTER_END_DATE}         2025-11-21
${HTML_DF}
${PARQUET_DF}


*** Test Cases ***
Open local HTML file


        ### 1. Open report.html in Chrome. ###

    ${abs_path}=            Normalize Path              ${REPORT_FILE}
    ${url} =                Set Variable                file:///${abs_path}
    Open Browser            ${url}                      Chrome
    Page Should Contain     DQE Automation


        ### 2. Locate the HTML table. ###

    ${table_element} =	    Get WebElement	            class:table


        ### 3. Read table data into DataFrame using your helper function passing a located table to a function. ###

    ${html_df} =            Get Html Dataframe          ${table_element}
    Set Suite Variable      ${HTML_DF}                  ${html_df}
    Log                     ${HTML_DF}


        ### 4. Read Parquet data and apply optional filtering using your helper function. ###

    ${abs_path}=            Normalize Path              ${PARQUET_FOLDER}
    ${parquet_df} =         Load Parquet Dataframe      ${abs_path}             columns=${PARQUET_COLUMNS}      start_date=${FILTER_START_DATE}    end_date=${FILTER_END_DATE}    date_column=${DATE_COLUMN}
    Set Suite Variable      ${PARQUET_DF}               ${parquet_df}
    Log                     ${PARQUET_DF}


        ### 5. Compare both DataFrames: ###
        #        Pass the test if they match.
        #        Fail the test if mismatches are found, showing the differences.

    ${diff_df} =            Verify Dataframes Match     ${HTML_DF}              ${PARQUET_DF}                   key_columns=${KEY_COLUMNS}
    Log                     ${diff_df}


        ### 6. Close the browser in teardown. ###

    Close Browser


*** Keywords ***
Get Html Dataframe
    [Arguments]    ${table_element}
    ${df}=         Read Html Table To Pandas        ${table_element}
    RETURN         ${df}

Load Parquet Dataframe
    [Arguments]    ${folder_path}                   ${columns}=None         ${start_date}=None      ${end_date}=None            ${date_column}=visit_date
    ${df}=         Read Parquet To Pandas           ${folder_path}          columns=${columns}      start_date=${start_date}    end_date=${end_date}            date_column=${date_column}
    RETURN         ${df}

Verify Dataframes Match
    [Arguments]    ${actual_df}    ${expected_df}    ${key_columns}=None
    ${diff_df}=    Compare Dataframes                ${actual_df}           ${expected_df}          key_columns=${key_columns}
    RETURN         ${diff_df}

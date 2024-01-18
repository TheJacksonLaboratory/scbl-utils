# scbl-utils

A set of command-line utilities that facilitates data processing in the Single Cell Biology Lab at the [Jackson Laboratory](https://www.jax.org/).

## Top-level Usage

```
scbl-utils [OPTIONS] COMMAND [ARGS]...
```

### Example Usage

```
scbl-utils samplesheet-from-gdrive /path/to/fastq_dir /path/to/another/fastq_dir /path/to/a_dir/of/fastq_dirs/*
```

### Top-level Options
- `--config-dir, -c`: Configuration directory containing files necessary for script to run. (default: `/sc/service/etc/.config/scbl-utils`)

### Commands

- #### **`samplesheet-from-gdrive`**

    Pull data from Google Drive and generate a `yml` samplesheet to be used as input for the [`nf-tenx`](https://github.com/TheJacksonLaboratory/nf-tenx) pipeline.
        
    ##### **Usage**

    ```{bash}
    scbl-utils samplesheet-from-gdrive [OPTIONS] FASTQDIRS
    ```

    ##### **Options**

    - `--outsheet, -o`: File path to save the resulting samplesheet. [default: `samplesheet.yml`]
    - `--reference-path-as-str, -s`: If possible, write the `reference_path` field of the outputted samplesheet as a string rather than a list of strings. This enables compatability with the current [`nf-tenx`](https://github.com/TheJacksonLaboratory/nf-tenx) pipeline and wll be deprecated in the future as the pipeline is updated.

    ##### **Requirements**
    
    For the script to work, the `config-dir` (as defined [here](#top-level-options)) must contain a directory called `google-drive`. In `{config-dir}/google-drive`, 3 files must exist:
    
    - `trackingsheet-spec.yml`: a specification that instructs the script on how to read data from the Google Sheet being used as the "sample tracking sheet". The specification must contain the following keys:
        - `id`: the Google Spreadsheet ID, found in https://docs.google.com/spreadsheets/d/`spreadsheet_id`/
        - `sheets`: a mapping of the ID of each worksheet (docs.google.com/spreadsheets/d/`spreadsheet_id`/edit#gid=`worksheet_id`) within the spreadsheet to another mapping of information about that sheet. The keys of this `dict` must contain:
            - `columns`: yet another mapping, this time mapping the column names in the sheet to how they should be renamed in the script. The union of all of the values of these mappings should minimally be `{10x_platform, sample_name, is_nuclei, libraries, project, species, n_cells, slide, area, tag_id}`, and `libraries` must exist in the sheets where `join == true` to join the sheets.
            - `header_row`: The index of the header row (0-based), which contains the column names
            - `join`: a `bool` indicating whether to join this sheet (along the columns) to the other sheets in the spreadsheet. Useful for spreadsheets with multiple sheets, but not every sheet shares the same index, meaning they are not necessarily joinable
        - `platform_to_lib_type`: another mapping, this time mapping the name of a 10X platform to the library type

            **Example `trackingsheet-spec.yml`:**

            ```{yml}
            id: <spreadsheet_id>
            sheets:
                0:
                    columns:
                        10X Platform: 10x_platform
                        Customer ID: sample_name
                        Is Nuclei: is_nuclei
                        Sample Name (SCBID): libraries
                        SCBL Project: project
                        Species: species
                        Targeted Cell Recovery: n_cells
                    header_row: 2
                    join: true
                2:
                    columns:
                        Sample Name (SCBID): libraries
                        Serial Number GEX Slide: slide
                    header_row: 2
                    join: true
                4:
                    columns:
                        Sample Name (SCBID): libraries
                        Position on Slide: area
                    header_row: 2
                    join: true
                5:
                    columns:
                        Customer ID: sub_sample_name
                        Pool ID: sample_name
                        SCID: libraries
                        Tag ID: tag_id
                        Tissue/Cell Type: description
                    header_row: 0
                    join: false
            platform_to_lib_type:
                3' RNA: Gene Expression
                3' RNA-HT: Gene Expression  
                5' RNA: Gene Expression
                5' RNA-HT: Gene Expression
                5' VDJ: Immune Profiling
                ATAC: Chromatin Accessibility
                ATAC v2: Chromatin Accessibility
                Automated RNA: Gene Expression
                Cell Surface: Antibody Capture
                CellPlex: Multiplexing Capture
                Flex: Gene Expression
                HTO: Multiplexing Capture
                LMO: Multiplexing Capture
                Multiome ATAC: Chromatin Accessibility
                Multiome RNA: Gene Expression
                RNA: Gene Expression
                RNA-HT: Gene Expression
                Visium CytAssist FFPE: CytAssist Gene Expression
                Visium FF: Spatial Gene Expression
                Visium FFPE: Spatial Gene Expression
            ```
    - `metricssheet-spec.yml`: a specification that instructs the script on how to read metrics sheets from Google Drive. This is useful for automated assignment of processing tool, tool version, and reference genome, as the script looks for old metrics spreadsheets within the same "SCBL Project" to assign these values when possible. The necessary keys are similar to `trackingsheet-spec.yml`:
        - `dir_id`: the ID of the Google Drive folder where delivered metrics are stored. Found in https://drive.google.com/drive/folders/`folderID`
        - `header_row`: the header row of the metrics sheets. This assumes that all sheets within all spreadsheets in the metrics delivery folder have the same header row
        - `columns`: just like `trackingsheet-spec.yml`, this is a mapping of the column names as they appear in the spreadsheets to how they should be named in the script. The union of these columns should minimally be `{project, tool, tool_version, reference, libraries}`. Because different metrics sheets have the `libraries` column as a different name, it may be necessary to add key-value pairs to this if the script throws a [`pandas`](https://pandas.pydata.org/docs/) error along the lines of `KeyError: '{key}'`. Because the script uses this mapping to determine what columns in the spreadsheet should go into a `pandas.DataFrame`, it will throw an error if it hasn't been informed that a certain column in the spreadsheet is really the `project` column, for example.

            **Example `metricssheet-spec.yml`**
            ```{yml}
            dir_id: <dir_id>
            header_row: 0
            columns:
                SCBL Project: project
                Processing Tool: tool
                Processing Tool Version: tool_version
                Processing Reference: reference
                Library ID(s): libraries
                Sample ID: libraries
            ```
    - `service-account.json`: A `json` file that stores credentials for a service account associated with a Google Cloud Project. This shouldn't be an issue, but if the script is throwing Google Drive login errors, this file might need to be regenerated and put in `{config-dir}/google-drive`. See the [gspread documentation](https://docs.gspread.org/en/v5.12.0/oauth2.html#service-account) for instructions.

    #### Adding New Tool Versions
    Because `scbl-utils samplesheet-from-gdrive` is designed to be used in conjunction with the [`nf-tenx`](https://github.com/TheJacksonLaboratory/nf-tenx) pipeline, it queries the the pipeline's `README` to know what versions of tools are available. If you are a maintainer of the `nf-tenx` project, you can simply add a new row to [this table](https://github.com/TheJacksonLaboratory/nf-tenx?tab=readme-ov-file#tools-available-on-jaxreg), and `scbl-utils` will now use that new row as the latest tool version for a given tool. Please ensure that you update the line above the table indicating when it was updated, and ensure that the tool is actually available in the [`singlecell` collection on JAXReg](https://jaxreg.jax.org/collections/3) (you won't be able to access this unless you are connected to JAX WiFi or the JAX VPN). This mechanism will likely be changed to something more robust in the future, perhaps pulling from JAXReg directly.

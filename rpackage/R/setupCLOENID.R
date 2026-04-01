setupCLONEID = function(host = 'localhost', port = '3306', user = NA, password = NA, database="CLONEID", schemaScript = "CLONEID_schema.sql", cellseg_input = NA, cellseg_output = NA, cellseg_tmp = NA, cellseg_backend = NA, cellseg_bucket = NA, cellseg_region = NA, cellseg_endpoint = NA, cellseg_input_prefix = NA, cellseg_output_prefix = NA){
    
    yaml_dir = paste0(system.file(package='cloneid'), '/config/config.yaml')
    yml = read_yaml(yaml_dir)
    
    if (is.na(host)) { host = yml$mysqlConnection$host }
    else { yml$mysqlConnection$host = host }
    
    if (is.na(port)) { port = yml$mysqlConnection$port }
    else { yml$mysqlConnection$port = port }
    
    if (is.na(user)) { user = yml$mysqlConnection$user }
    else { yml$mysqlConnection$user = user }
    
    if (is.na(password)) { password = yml$mysqlConnection$password }
    else { yml$mysqlConnection$password = password }
    
    if (is.na(database)) { database = yml$mysqlConnection$database }
    else { yml$mysqlConnection$database = database }
    
    if (is.na(schemaScript)) { schemaScript = yml$mysqlConnection$schemaScript }
    else { yml$mysqlConnection$schemaScript = schemaScript }

    if (!is.na(cellseg_input))  { yml$cellSegmentation$input  = cellseg_input  }
    if (!is.na(cellseg_output)) { yml$cellSegmentation$output = cellseg_output }
    if (!is.na(cellseg_tmp))    { yml$cellSegmentation$tmp    = cellseg_tmp    }
    if (!is.na(cellseg_backend))       { yml$cellSegmentation$backend      = cellseg_backend }
    if (!is.na(cellseg_bucket))        { yml$cellSegmentation$bucket       = cellseg_bucket }
    if (!is.na(cellseg_region))        { yml$cellSegmentation$region       = cellseg_region }
    if (!is.na(cellseg_endpoint))      { yml$cellSegmentation$endpoint     = cellseg_endpoint }
    if (!is.na(cellseg_input_prefix))  { yml$cellSegmentation$inputPrefix  = cellseg_input_prefix }
    if (!is.na(cellseg_output_prefix)) { yml$cellSegmentation$outputPrefix = cellseg_output_prefix }

    write_yaml(yml, yaml_dir)

    return(yml)    
}

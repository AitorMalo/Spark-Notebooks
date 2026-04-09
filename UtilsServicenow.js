const UtilsSnowflake = require('./UtilsSnowflake');
require('dotenv').config({ path: `./configuration/${process.env.environment}.env`, override: true });
const fetch = require("node-fetch");
const jwt = require("jsonwebtoken");
const UtilsYaml = require("./UtilsYaml");
const Utils= require("./Utils");

const { HttpsProxyAgent } = require("https-proxy-agent"); 

/* ----------------------------------------------------------
   CREATE + SAVE TICKET
------------------------------------------------------------- */
async function CreateSaveTicket(
    context,
    pCountryConfig,
    pTicketType,
    pProcess,
    pShortDescription,
    pDescription
) {

    context.log(`Inicia CreateSaveTicket ${pTicketType} ${pShortDescription}`);

    const ticketConfig = UtilsYaml.ReadConfig("ServicenowTicket.yaml");
    const vTicketConfig = ticketConfig[pTicketType];

    if (!vTicketConfig) {
        throw new Error(`Ticket type not configured: ${pTicketType}`);
    }

    const vAssignmentgroup =
        vTicketConfig['assignment_group'][pCountryConfig['short_name']];
    
    // context.log('CreateSaveTicket -->',pCountryConfig['short_name'], vAssignmentgroup, pTicketType);
    
    let vExist;

    try {
         
         vExist = await TicketExists(context, pCountryConfig['short_name'], new Date(), pTicketType);
         
    } catch(error) {
        context.log(`Error en TicketExists ${error}`);
        throw new Error(`Error en TicketExists ${error}`);
    }

    context.log('vExist', vExist);

    let vTicket = { ticketNumber: '', ticketSysId: ''};
 
    if ( !vExist ) {

       context.log('Ticket no existe');

       try {
             context.log('llama a createServicenowTicket');
             vTicket = await createServicenowTicket(
                 context,
                 pCountryConfig.code,
                 pTicketType,
                 pShortDescription,
                 pDescription,
                 vAssignmentgroup
             );

             // ✅ ÚNICO LOG QUE SE MANTIENE
             context.log(`✅ Ticket creado: ${vTicket.ticketNumber}`);
        } catch(error) {
            context.log("Error creating ServiceNow ticket");
            throw new Error(`ServiceNow integration error ${error}`);
        }
 
        if (!vTicket?.ticketSysId) {
            throw new Error("Invalid ServiceNow response");
        }

        // ----- Insert ticket info in table BAU_SNOW_TICKETS
        const vValuesInsert = [   vTicket['ticketSysId'], // ID
                            vTicket['ticketNumber'], // NUMBER
                            vTicketConfig['u_type'][0].toUpperCase(), // TYPE S/I soporte/incidencia
                            pTicketType, // TICKET_CATEGORY
                            new Date(), // CREATED_DATE
                            pProcess, // CREATED_PROCESS
                            pShortDescription, // SHORT_DESCRIPTION
                            pCountryConfig['short_name'], // COUNTRY
                            vAssignmentgroup, // ASSIGNMENT GROUP
                            null, // CLOSED_DATE
                            'A']; // STATUS - Abierto/Cerrado
   
        context.log("Insertando registro en BAU_SNOW_TICKETS");
        UtilsSnowflake.InsertRowSnowflake('BAU_SNOW_TICKETS', vValuesInsert);

    } else {
        context.log(`✅ No se ha creado ticket porque ya existía`);
    }
    return vTicket;
}

/* ----------------------------------------------------------
   GET COMPANY
------------------------------------------------------------- */
function GetCompany(context, pCountryConfig, pTicketConfig) {

    const countryShortName = pCountryConfig?.short_name;

    if (
        pTicketConfig?.company &&
        countryShortName &&
        pTicketConfig.company[countryShortName]
    ) {
        return pTicketConfig.company[countryShortName];
    }

    return pCountryConfig?.company;
}

/* ----------------------------------------------------------
   GET ACCESS TOKEN
------------------------------------------------------------- */
async function getAccessToken() {

    const tokenConfig = UtilsYaml.ReadConfig("TokenConfig.yaml");

    const URL_TOKEN = process.env.SNOW_TOKEN_URL;
    const proxy = process.env.SNOW_PROXY;

    const now = Math.floor(Date.now() / 1000);

    const jwtHeader = {
        ...tokenConfig.jwt.header,
        kid: process.env.SNOW_JWT_KID
    };

    context.log('Key starts with:', process.env.SNOW_JWT_PRIVATE_KEY?.substring(0, 30));
    
    const token = jwt.sign(
        {
            aud: process.env.SNOW_CLIENT_ID,
            sub: process.env.SNOW_JWT_SUB,
            iss: process.env.SNOW_JWT_ISS,
            exp: now + 300
        },
        process.env.SNOW_JWT_PRIVATE_KEY.replace(/\\n/g, '\n'),
        {
            algorithm: tokenConfig.jwt.header.alg,
            header: jwtHeader
        }
    );

    const body =
        `grant_type=${tokenConfig.access_token.payload.grand_type}` +
        `&assertion=${token}` +
        `&client_id=${process.env.SNOW_CLIENT_ID}` +
        `&client_secret=${process.env.SNOW_CLIENT_SECRET}`;

    const agent = proxy
        ? new HttpsProxyAgent(`http://${proxy}`)
        : undefined;

    const response = await fetch(URL_TOKEN, {
        method: "POST",
        headers: tokenConfig.access_token.header,
        body: body,
        agent: agent
    });

    const text = await response.text();

    if (!response.ok) {
        throw new Error("Failed to obtain access token");
    }

    return JSON.parse(text).access_token;
}

/* ----------------------------------------------------------
   TICKET EXISTS
------------------------------------------------------------- */
async function TicketExists(context, pCountry, pDate, pTicketCategory) {
 
    const vQuery = `SELECT ID
                    FROM BAU_SNOW_TICKETS
                    WHERE COUNTRY = ?
                    AND DATE_TRUNC('DAY', CREATED_DATE) = DATE_TRUNC('DAY', TO_TIMESTAMP(?))
                    AND TICKET_CATEGORY = ?
                    `;

    const vValues = [pCountry, pDate, pTicketCategory];

    const vResult = await UtilsSnowflake.ExecuteQuerySnowflake(vQuery, vValues);
   
    let vExist = false;
 
    if (vResult.length > 0) {
        vExist = true;
    }
    
    context.log('retorna TicketExists ',vExist);

    return vExist;
}

/* ----------------------------------------------------------
   CREATE SERVICENOW TICKET
------------------------------------------------------------- */
async function createServicenowTicket(
    context,
    country,
    ticketType,
    short,
    description,
    assignmentGroup
) {

    const apiConfig = UtilsYaml.ReadConfig("ServicenowConnect.yaml");
    const ticketConfig = UtilsYaml.ReadConfig("ServicenowTicket.yaml");
    const countriesConfig = UtilsYaml.ReadConfig("CountriesConfig.yaml");
    const userConfig = UtilsYaml.ReadConfig("UserConfig.yaml");

    const url = apiConfig.api_connect.url_create;
    const proxy = apiConfig.api_connect.proxy;

    const countryConfig = countriesConfig[country];
    const ticketTypeConfig = ticketConfig[ticketType];
    const commonConfig = ticketConfig.common;

    if (!countryConfig) {
        throw new Error("Country not configured");
    }

    if (!ticketTypeConfig) {
        throw new Error("Ticket type not configured");
    }

    const accessToken = await getAccessToken();

    const agent = proxy
        ? new HttpsProxyAgent(`http://${proxy}`)
        : undefined;

    const payload = {
        company: GetCompany(context, countryConfig, ticketTypeConfig),
        u_type: ticketTypeConfig.u_type,
        short_description: short,
        description: description,
        cmdb_ci: commonConfig?.cmdb_ci,
        u_source: commonConfig?.u_source,
        u_integration_system: commonConfig?.u_integration_system,
        assignment_group: assignmentGroup,
        caller_id: userConfig?.correo
    };

    if (ticketTypeConfig.close_ticket) {
        payload.assigned_to = ticketTypeConfig.assigned_to;
    }

    const response = await fetch(url, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            Accept: "application/json",
            "Content-Type": "application/json"
        },
        body: JSON.stringify(payload),
        agent: agent
    });

    const text = await response.text();

    if (response.status !== 201) {
         throw new Error("Failed to create ServiceNow ticket");
    }

    const data = JSON.parse(text);

    return {
        ticketNumber: data?.result?.number,
        ticketSysId: data?.result?.sys_id
    };
}


module.exports = {
    CreateSaveTicket,
    getAccessToken,
    createServicenowTicket,
    TicketExists
};

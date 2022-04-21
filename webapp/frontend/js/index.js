const SERVER_IP = "localhost";
const SERVER_PORT = "7777"
const SERVER_URL = SERVER_IP + ":" + SERVER_PORT;


$(document).ready(function(){
    get_cities();

    $('#citySelection').change(function(){
        // Removing zip options from second selection when city changes
        var select = document.getElementById("zipcodeSelection");
        select.innerHTML = "<option selected disabled>Select a Zipcode</option>"

        city = select.value;
        city = document.getElementById("citySelection").value
        console.log(city)

        get_zipcodes(city)
    });
});


function get_cities(){
    var xhttp = new XMLHttpRequest();

    xhttp.onreadystatechange = function () {
        if (xhttp.readyState == 4 && (xhttp.status == 200 || xhttp.status == 0)) {
            var response_data = JSON.parse(xhttp.responseText);
            if (response_data.status == "OK") {

                    var select = document.getElementById("citySelection");

                    for(var i = 0; i <  response_data.num_cities; i++) {
                        var opt = response_data.cities[i];
                        select.innerHTML += "<option value=\"" + opt + "\">" + opt + "</option>";
                    }
                }
            }
        }

    xhttp.open("GET", "http://" + SERVER_URL + "/getcities", true);
    xhttp.send()
}


function get_zipcodes(city){
    var xhttp = new XMLHttpRequest();
    var formdata_city = new FormData();
    formdata_city.append("city", city);

    xhttp.onreadystatechange = function () {
        if (xhttp.readyState == 4 && (xhttp.status == 200 || xhttp.status == 0)) {
            var response_data = JSON.parse(xhttp.responseText);
            if (response_data.status == "OK") {

                    var select = document.getElementById("zipcodeSelection");

                    for(var i = 0; i <  response_data.num_zipcodes; i++) {
                        var opt = response_data.zipcodes[i];
                        select.innerHTML += "<option value=\"" + opt + "\">" + opt + "</option>";
                    }
                }
            }
        }

    xhttp.open("POST", "http://" + SERVER_URL + "/getzipcodes", true);
    // xhttp.withCredentials = true;
    xhttp.send(formdata_city)
}



function trigger_rest_names(){
    city = document.getElementById("citySelection").value;
    zipcode = document.getElementById("zipcodeSelection").value;
    get_rest_names(city, zipcode);
}


function get_rest_names(city, zipcode) {
    console.log("rest names called");
    console.log("city - ", city);
    console.log("zipcode - ", zipcode);

    var form_data = new FormData();
    form_data.append("city", city);
    form_data.append("zipcode", zipcode);

    var xhttp = new XMLHttpRequest();
    xhttp.open("POST", "http://" + SERVER_URL + "/getrestnames", true);
    // xhttp.withCredentials = true;
    xhttp.send(form_data);

    xhttp.onreadystatechange = function () {
        if (xhttp.readyState == 4 && (xhttp.status == 200 || xhttp.status == 0)) {
            // $("body").prepend(overview_container);
            var datarestnames = JSON.parse(xhttp.responseText.replace(/\bNaN\b/g, "null"));
            console.log(datarestnames);

            if (datarestnames) {
                populate_data_table("restnames", datarestnames);
            }
            else {
                // alert("File not found");
                console.log("error in restnames")
                // window.location.href = "./index.html";
            }
        }
    }
}




function get_menu(row_data) {
    console.log("menu called");
    var form_data = new FormData();
    form_data.append("zipcode", row_data[2]);
    form_data.append("city", row_data[3]);
    // form_data.append("rest_name", row_data[0]);
    form_data.append("business_id", row_data[0]);

    var xhttp = new XMLHttpRequest();
    xhttp.open("POST", "http://" + SERVER_URL + "/getmenu", true);
    xhttp.send(form_data);

    xhttp.onreadystatechange = function () {
        if (xhttp.readyState == 4 && (xhttp.status == 200 || xhttp.status == 0)) {
            // $("body").prepend(overview_container);
            var datamenu = JSON.parse(xhttp.responseText.replace(/\bNaN\b/g, "null"));
            console.log(datamenu);

            if (datamenu) {
                populate_data_table("menu", datamenu);
            }
            else {
                // alert("File not found");
                console.log("error in menu")
                // window.location.href = "./index.html";
            }
        }
    }
}


function populate_data_table(id, data) {
    try {
        if ($.fn.dataTable.isDataTable('#table-' + id)) {
            $('#table-' + id).DataTable().destroy();
            console.log("destroyed");
        }
    } catch (error) {
        console.log("table not found");
    }

    if ("restnames" == id){
        var order_col_num = 1; // change when true data is uploaded (for stars)
    }
    else{
        var order_col_num = 2;
    }
    

    $('#table-' + id).DataTable({
        "order": [[ order_col_num, "desc" ]],
        data: data.data,
        columns: data.coldefs,
        
        // "bLengthChange": false,
        // "scrollX": true,
        "paging": true,
        "pagingType": "simple",
        "filter": false,
        "info": false,

        // to highlight selected row
        select: {
            style: 'os',
            className: 'focusedRow',
            selector: 'td:last-child a'
        },

        "columnDefs": [
            {
                "targets": [ 0 ],
                "visible": false
            }
        ]
    });

    console.log("hi");
}


$(document).ready(function() {  
    $('#table-restnames').on('click', 'tr', function () {
        // highlight the selected row
        $('#table-restnames tbody > tr').removeClass('table-active');
        $(this).addClass('table-active');

        var table = $('#table-restnames').DataTable();
        var data = table.row( this ).data();
        console.log( 'You clicked on '+data[0]+'\'s row' );
        get_menu(data)
    } );
});
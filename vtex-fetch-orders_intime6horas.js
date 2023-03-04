console.clear();

var request = require('sync-request');
var mysql = require('mysql');
var fs = require('fs');
var cron = require("node-cron");
var sleep = require('system-sleep');
var schedule = require('node-schedule');

global.mysqlData = { host: "bd-integracion-plataformas.cczld2puss05.us-east-1.rds.amazonaws.com", user: "integracion", password: "ne4pass", database: 'forge' };
global.bxGeo = JSON.parse(fs.readFileSync('../dados/bx-geo.json'));
global.postalCodes = JSON.parse(fs.readFileSync('../dados/postalCodeNew.json'));

console.clear();
console.log(new Date().toString(), "Servicio de integracion activa");

var reqBxGeo = request('GET', 'https://bx-tracking.bluex.cl/bx-geo/states', { headers: { 'bx-usercode': '15083', 'bx-token': '53327710dec54bbffbdb84cabebee584', 'bx-client_account': '96801150-11-8', 'content-type': 'application/json' }, });
var bxGeo = JSON.parse(reqBxGeo.body.toString('utf-8'));
var bxGeoJson = [];
bxGeo.data.forEach((geo, indice) => {
	geo.states.forEach((estado, indice) => {
		var estadoName = parsed(estado.name);
		var estadoCode = estado.code;
		estado.ciudades.forEach((cidades, indice) => {
			var cidadeName = parsed(cidades.name);
			var cidadeCode = cidades.code;
			var defaultDistrict = cidades.defaultDistrict;
			cidades.districts.forEach((distritos, indice) => {
				var distritosName = parsed(distritos.name);
				var distritosCode = distritos.code;
				bxGeoJson.push({ "estadoName": estadoName, "estadoCode": estadoCode, "cidadeName": cidadeName, "cidadeCode": cidadeCode, "distritosName": distritosName, "distritosCode": distritosCode, "defaultDistrict": defaultDistrict });
			})
		})
	})
});
class integracaoTray {
	constructor(dadosCliente) {
		global.cliente = dadosCliente;
		global.datas = JSON.parse(fs.readFileSync('/var/www/integraciones/vtex/orders/datas.json'));
	};
	buscaPedidosTodos() {
		try {
			//--- fecha con dias menos
			let d = new Date();
			d.setDate(d.getDate() - 3);
			var dateini = d.toISOString().slice(0, 10)
			
			//--- fecha hoy
			const todayDate = new Date().toISOString().slice(0, 10);
			var datefin = todayDate

			// --- Hora actual
			var hoursNow = new Date();
			hoursNow.setHours(hoursNow.getHours());
			var setHoursNow = hoursNow.toISOString().slice(11, 16)
			var setDayNow = hoursNow.toISOString().slice(0, 10) 
			
			//-- Hora restada
			var hoursLess = new Date();
			hoursLess.setHours(hoursLess.getHours() - 5); // restar -3 para tener la hora de Chile
			var sethoursLess = hoursLess.toISOString().slice(11, 16)
			var setDayLess = hoursLess.toISOString().slice(0, 10)
			
			// --
			var pagina = 0;
			var continua = true;
			var pedidos = [];
			console.log("dateini: ",setDayLess +" : "+ sethoursLess);
			console.log("datefin: ",setDayNow +" : "+setHoursNow);
			
			while (continua == true) {
console.log("pagina",pagina)
				pagina++;
//BACKUP  const url = `https://${cliente.accountName}.${cliente.environment}.com/api/oms/pvt/orders?page=${pagina}&f_status=${cliente.statusVtex}&per_page=50&f_authorizedDate=authorizedDate:['${datas.inicial} TO  ${datas.final} ']`

				// const url = `https://${cliente.accountName}.${cliente.environment}.com/api/oms/pvt/orders?page=${pagina}&f_status=${cliente.statusVtex}&per_page=50`
				const url = `https://${cliente.accountName}.${cliente.environment}.com/api/oms/pvt/orders?page=${pagina}&f_status=${cliente.statusVtex}&per_page=50&f_invoicedDate=invoicedDate:[${setDayLess}T${sethoursLess}:00.000Z TO ${setDayNow}T${setHoursNow}:00.999Z]`
				console.log("url ", url)
const headers =  { 'x-vtex-api-appkey': cliente.key, 'x-vtex-api-apptoken': cliente.token, 'Accept': 'application/json', 'Content-Type': 'application/json' }
console.log("headers",headers)

				var reqPedidos = request('GET', url, { headers: headers });
console.log("reqPedidos.statusCode",reqPedidos.statusCode)

				if (reqPedidos.statusCode == 200) {

					var retorno = JSON.parse(reqPedidos.body.toString('utf-8'));
					try {
						if (retorno) {
							if (retorno.error) {
								registerLogs(cliente.storeID, '', 'Unauthorized access', 'Vtex Login');
								continua = false;
							} else {
								retorno.list.forEach((pedido, indice) => {
									
									pedidos.push({ 'orderId': pedido.orderId, 'status': pedido.status });
									
								})
								if (!retorno.paging) continua = false
								if (retorno.paging.pages <= retorno.paging.currentPage || retorno.paging.currentPage >= 5) continua = false
								if (retorno.paging.pages <= retorno.paging.currentPage || retorno.paging.currentPage >= 5) continua = false
							}
						}
					} catch (err) {
						continua = false;
					}
					sleep(1000);
				} else {
					continua = false;

				}


			}

			this.pedidos = pedidos;
			console.log(new Date(), ' - Cantidad de pedidos: ', pedidos.length);
		} catch (err) {
			console.log(err);
		}
	};
	buscaPedidosDetalhes() {
		try {
			this.pedidos.forEach((pedido, indice) => {
				var reqDetalhes = request('GET', 'https://' + cliente.accountName + '.' + cliente.environment + '.com/api/oms/pvt/orders/' + pedido.orderId, { headers: { 'x-vtex-api-appkey': cliente.key, 'x-vtex-api-apptoken': cliente.token, 'Accept': 'application/json', 'Content-Type': 'application/json' }, });
				var detalhesPedido = JSON.parse(reqDetalhes.body.toString('utf-8'));
				//console.log('detalhesPedido-line-99', detalhesPedido);
				sleep(200);
				var selectedSla = detalhesPedido.shippingData.logisticsInfo[0].selectedSla;
				if (selectedSla == "Bluexpress" || selectedSla == "BlueSameDay" || selectedSla == "BluePriority" || selectedSla == "Despacho Express" || ((selectedSla == "Normal" || selectedSla == "Sameday/Next Day") && cliente.accountName == "intimecl")) {
					console.log(indice,' - ',new Date(),' - ',selectedSla, ' - ', detalhesPedido.orderId);
					//console.log("detalhesPedido", JSON.stringify(detalhesPedido));
					this.integrarPedido(pedido, cliente, detalhesPedido);
				}

			})
		} catch (err) {
			console.log('buscaPedidosDetalhes', err)
		}
	};
	integrarPedido(pedido, cliente, pedidoDados) {
		try {
			//console.log('pedidoDados-line-114', pedidoDados);
			if (!pedidoDados.shippingData.selectedAddresses[0].complement) pedidoDados.shippingData.selectedAddresses[0].complement = '';
			var jsonPedido = this.cadastraIntegracao(cliente, pedidoDados, pedidoDados.packageAttachment.packages[0].invoiceNumber);
		} catch (err) {
			console.log('integrarPedido', err)
		}
	};
	cadastraIntegracao(cliente, pedidoDados, invoiceNumber) {
		try {
			var bxSelecionado = '';
			if (pedidoDados.shippingData.selectedAddresses[0].neighborhood) {
				pedidoDados.shippingData.selectedAddresses[0].neighborhood = parsed(pedidoDados.shippingData.selectedAddresses[0].neighborhood).replace('llay-llay', 'llaillay');
				pedidoDados.shippingData.selectedAddresses[0].neighborhood = parsed(pedidoDados.shippingData.selectedAddresses[0].neighborhood).replace('la calera', 'calera');
				pedidoDados.shippingData.selectedAddresses[0].neighborhood = parsed(pedidoDados.shippingData.selectedAddresses[0].neighborhood).replace('marchigue', 'marchihue')
			}

			bxGeoJson.forEach((bxGEO, indice) => {
				if (parsed(bxGEO.distritosName) == parsed(pedidoDados.shippingData.selectedAddresses[0].neighborhood)) {
					if (parsed(bxGEO.cidadeName) == parsed(pedidoDados.shippingData.selectedAddresses[0].neighborhood)) {
						bxSelecionado = bxGEO;
					}
				}
			})
			if (bxSelecionado == '') {
				bxGeoJson.forEach((bxGEO, indice) => {
					if (parsed(bxGEO.cidadeName) == parsed(pedidoDados.shippingData.selectedAddresses[0].neighborhood)) {
						bxSelecionado = bxGEO;
					}
				})
			}
			if (bxSelecionado == '') {
				bxGeoJson.forEach((bxGEO, indice) => {
					if (parsed(bxGEO.distritosName) == parsed(pedidoDados.shippingData.selectedAddresses[0].neighborhood)) {
						bxSelecionado = bxGEO;
					}
				})
			}
			if (!bxSelecionado) {
				registerLogs(cliente.storeID, pedidoDados.orderId, 'Invalid Address', 'Registration data');
			} else {
				var valFlete = '';
				pedidoDados.totals.forEach((totals, indice) => {
					if (totals.id == 'Shipping')
						valFlete = totals.value / 100;
				});

				var orderItens = [];
				pedidoDados.items.forEach((orderItem, indice) => {
					orderItens.push({ "quantity": orderItem.quantity, "name": orderItem.name, "height": orderItem.additionalInfo.dimension.height, "length": orderItem.additionalInfo.dimension.length, "weight": orderItem.additionalInfo.dimension.weight, "width": orderItem.additionalInfo.dimension.width });
				});

				var dadosPedido = {
					"identificador": pedidoDados.orderId,
					"sequence": pedidoDados.sequence,
					"destinatario": parsed(pedidoDados.shippingData.selectedAddresses[0].receiverName).replace("'", "`"),
					"cpf_cnpj": pedidoDados.clientProfileData.document,
					"endereco": parsed(pedidoDados.shippingData.selectedAddresses[0].street),
					"numero": pedidoDados.shippingData.selectedAddresses[0].number,
					"bairro": parsed(pedidoDados.shippingData.selectedAddresses[0].neighborhood),
					"cidade": parsed(pedidoDados.shippingData.selectedAddresses[0].city),
					"uf": parsed(pedidoDados.shippingData.selectedAddresses[0].state),
					"cep": pedidoDados.shippingData.selectedAddresses[0].postalCode,
					"complemento": parsed(pedidoDados.shippingData.selectedAddresses[0].complement),
					"email": pedidoDados.clientProfileData.email,
					"telefone": pedidoDados.clientProfileData.phone,
					"produto": parsed(pedidoDados.items[0].name),
					"peso": pedidoDados.items[0].additionalInfo.dimension.weight,
					"comprimento": pedidoDados.items[0].additionalInfo.dimension.length,
					"largura": pedidoDados.items[0].additionalInfo.dimension.width,
					"altura": pedidoDados.items[0].additionalInfo.dimension.height,
					"invoiceNumber": invoiceNumber,
					"serviceType": pedidoDados.shippingData.logisticsInfo[0].selectedSla,
					"valPedido": pedidoDados.value,
					"valFrete": valFlete,
					"cliente": cliente,
					"bxGEO": bxSelecionado,
					"orderItens": orderItens,
				}

				//console.log('dadosPedido-line-192', dadosPedido);

				if (dadosPedido) {
					try {
						var conn = mysql.createConnection(mysqlData);
						conn.connect();
						conn.query("SELECT o.trackingNumber from forge.orders o WHERE o.orderId = '" + pedidoDados.orderId + "'", function (error, results, fields) {
							console.log('results-line-197', results);
							if (!results[0]) {
								try {
									console.log(new Date(), 'gerando arquivos', dadosPedido.identificador);
									fs.writeFile("/var/www/integraciones/vtex/orders/pendentes/" + dadosPedido.identificador + "-" + cliente.storeID + ".json", JSON.stringify(dadosPedido), (err) => { });
								} catch (err) {
									console.log(err)
								}
							} else {
								console.log('Ya existe la orden ', pedidoDados.orderId)
								//
								//								fs.writeFile("/var/www/integraciones/vtex/orders/duplicadas/"+dadosPedido.identificador+"-"+cliente.storeID+".json", JSON.stringify(dadosPedido),(err) =>{});
								//								orderUpdate(dadosPedido,results[0].trackingNumber)
							}
						});
						sleep(150);
						conn.end();
					} catch (err) {
						console.log(err);
					}
				}
			}
		} catch (err) {
			console.log(err);
		}
	}
}
function init() {
	datas = { 'inicial': new Date(Date.now() - 6 * 24 * 60 * 60 * 1000), 'final': new Date(Date.now()) };
	fs.writeFile('/var/www/integraciones/vtex/orders/datas.json', JSON.stringify(datas), (err) => { });
	sleep(1000);
	console.log(new Date().toString(), "Inicio de la integracion para nuevos pedidos");
	var conn = mysql.createConnection(mysqlData);
	conn.connect();
	conn.query("SELECT s.id storeID, s.key, s.token, s.accountName, s.environment, u.name, u.email, s.statusVtex, s.district_id, bx_token, bx_user_code, bx_client_account, bx_user_name, d.name District, d.refCode DistrictCode, st.refCode stateId, s.name pickupName, CONCAT(s.street, ',', s.number) address, s.responsableFullName, s.responsablePhone FROM forge.stores s JOIN forge.users u ON u.id = s.user_id JOIN districts d ON d.id = s.district_id JOIN cities c ON c.id = d.city_id JOIN states st ON st.id = c.state_id WHERE TYPE = 'vtex' AND s.id = 286", function (error, results, fields) {
		fs.writeFile('/var/www/integraciones/vtex/orders/clientes.json', JSON.stringify(results), (err) => { });
	});
	sleep(2000);
	conn.end();
}
function iniciaIntegracao() {
	try {
		clientesJson = JSON.parse(fs.readFileSync('/var/www/integraciones/vtex/orders/clientes.json', 'utf8'))
		clientesJson.forEach((cliente, indice) => {
			console.log(new Date().toString(), 'integrando cliente', cliente.accountName)
			tray = new integracaoTray(cliente);
			tray.buscaPedidosTodos();
			tray.buscaPedidosDetalhes();
			console.log(new Date().toString(), 'Finalizada integracao cliente', cliente.accountName)
			sleep(30000)
		});
	} catch (err) {
		console.log(err);
	}
}
function parsed(text) {
	try {
		text = text.trim();
		const parsed = text.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
		return parsed.toLowerCase();
	} catch (err) { }
}

function orderUpdate(orderDetail, trackingNumber) {
	try {
		var orderUpdate = request('PATCH', 'https://' + orderDetail.cliente.accountName + '.' + orderDetail.cliente.environment + '.com/api/oms/pvt/orders/' + orderDetail.identificador + '/invoice/' + orderDetail.invoiceNumber,
			{
				headers: { 'x-vtex-api-appkey': orderDetail.cliente.key, 'x-vtex-api-apptoken': orderDetail.cliente.token, 'Accept': 'application/json', 'Content-Type': 'application/json' },
				json: { 'trackingNumber': trackingNumber, 'trackingUrl': 'https://www.bluex.cl/seguimiento/?n_seguimiento=' + trackingNumber, 'courier': 'BlueExpress' }
			}
		);
		var resultUpdate = orderUpdate.body.toString('utf-8');
		if (fs.existsSync('/var/www/integraciones/vtex/orders/pendentes/' + orderDetail.identificador + "-" + orderDetail.cliente.storeID + ".json")) {
			fs.rename('/var/www/integraciones/vtex/orders/pendentes/' + orderDetail.identificador + "-" + orderDetail.cliente.storeID + ".json", '/var/www/integraciones/vtex/orders/finalizados/' + orderDetail.identificador + "-" + orderDetail.cliente.storeID + ".json", function (err) { });
		} else {
			console.log('/var/www/integraciones/vtex/orders/pendentes/' + orderDetail.identificador + "-" + orderDetail.cliente.storeID + ".json");
		}
		console.log(resultUpdate);
	} catch (err) {
		if (fs.existsSync('/var/www/integraciones/vtex/orders/pendentes/' + orderDetail.identificador + "-" + orderDetail.cliente.storeID + ".json")) {
			fs.rename('/var/www/integraciones/vtex/orders/pendentes/' + orderDetail.identificador + "-" + orderDetail.cliente.storeID + ".json", '/var/www/integraciones/vtex/orders/error/' + orderDetail.identificador + "-" + orderDetail.cliente.storeID + ".json", function (err) { });
		} else {
			console.log('/var/www/integraciones/vtex/orders/pendentes/' + orderDetail.identificador + "-" + orderDetail.cliente.storeID + ".json");
		}
		console.log('orderUpdate', orderUpdate.body.toString('utf-8'), err);
	}
}

//var eventInit = schedule.scheduleJob("*/30 * * * *", function() {
//		init();
//		sleep(10000)
//		iniciaIntegracao();
//	})
init();
sleep(10000)
iniciaIntegracao();

async function registerLogs(storeID, orderNumber, error, source) {
	var conn = mysql.createConnection(mysqlData);
	conn.query("INSERT INTO `logs`( Store_ID,Order_Number, Description,Source_Error)VALUES (" + storeID + ", '" + orderNumber + "', '" + error + "','" + source + "')", function (error, results, fields) { })
	conn.end();
}


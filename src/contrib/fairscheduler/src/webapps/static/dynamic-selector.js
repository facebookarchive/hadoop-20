// Note: var pools and var prios (optionally var advanced) must be defined
//		 in parent file.
// Ex: var pools = ['pool1','root']; var prios = ['LOW','HIGH']; var advanced = false;


// ------------ Setup ------------

if (document.getElementById && document.createElement) {
	var selPool = document.createElement('SELECT');
	for (var i in pools) {
		var opt = document.createElement('OPTION');
		opt.text = opt.value = opt.id = pools[i];
		selPool.add(opt, null);
	}

	var selPrio = document.createElement('SELECT');
	for (var i in prios) {
		var opt = document.createElement('OPTION');
		opt.text = opt.value = opt.id = prios[i];
		selPrio.add(opt, null);
	}

	selPool.onchange = selPrio.onchange = setNewValue;

	// I appropriated these just to keep track of whether or not I'm using them
	// @see isEditing()
	selPool.disabled = selPrio.disabled = true;
}

document.onclick = catchClick;


// ------------ Functions ------------

function isEditing () {
	return !(selPool.disabled && selPrio.disabled);
}

function activeSel () {
	if (!selPool.disabled && !selPrio.disabled) {
		var err = "Error: Multiple dynamic selectors are active"
										+ "(should not happen)";
		console.log(err);
		return null;
	}

	if (!selPool.disabled) return selPool;
	if (!selPrio.disabled) return selPrio;
	return null;
}

function setNewValue (e) {
	var sel = e.target;
	var row = (sel.parentNode).parentNode;
	var jobID = row.id;

	switch (sel) {
		case selPool:
			var path = '/fairscheduler?setPool='
						+ sel.value
						+ '&jobid='
						+ jobID;
			break;
		case selPrio:
			var path = '/fairscheduler?setPriority='
						+ sel.value
						+ '&jobid='
						+ jobID;
			break;
		default:
			console.log('Unexpected input to function');
			return false;
	}
	if (advanced) {
		path += '&advanced';
	}

	window.location = path;
	removeSelector(sel);
}

function insertSelector (elem) {
	if (isEditing() || (elem == null) || (elem.tagName != 'SPAN')) {
		return false;
	}

	switch (elem.id) {
		case 'DYN_POOL':
			var sel = selPool;
			break;
		case 'DYN_PRIO':
			var sel = selPrio;
			break;
		default:
			return false;
	}

	var initialValue = elem.innerHTML;
	var opt = sel.namedItem(initialValue);
	if (opt) {
		sel.selectedIndex = opt.index;
	} else {
		console.log('Error: no such element '
					+ initialValue
					+ ' in the list '
					+ sel);
	}

	var parent = elem.parentNode;
	parent.insertBefore(sel,elem);
	parent.removeChild(elem);
	parent.value = sel;
	parent.focus();

	sel.disabled = false; // Just to keep track that we're using this guy
}

function removeSelector (sel) {
	if (!isEditing() || (sel == null) || (sel.tagName != 'SELECT')) {
		return false;
	}

	var text = sel.options[sel.selectedIndex].text;
	var parent = sel.parentNode;

	var div = document.createElement('SPAN');
	div.className = 'fake-link';
	div.innerHTML = text;
	switch (sel) {
		case selPool:
			div.id = "DYN_POOL";
			break;
		case selPrio:
			div.id = "DYN_PRIO";
			break;
		default:
			console.log("Unexpected switch object being torn down: " + sel);
	}

	parent.insertBefore(div, sel);
	parent.removeChild(sel);

	sel.disabled = true; // Just to keep track that we're not using this guy
}

function catchClick (e) {
	if (!e) var obj = window.event.srcElement;
	else var obj = e.target;
	if (obj.tagName == 'OPTION') {
		// Necessary thanks to Mozilla's brilliant engineers
		obj = obj.parentNode;
	}

	if (!isEditing()) {
		insertSelector(obj);
	} else {
		if (obj.tagName != 'SELECT') {
			removeSelector(activeSel());
		}
	}
}

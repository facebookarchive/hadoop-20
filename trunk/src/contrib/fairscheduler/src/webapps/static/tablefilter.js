/**
 *  Table filtering functionality.
 *
 * @author schober
 */

var filteredTables = []; // array of Table objects


/**
 *  ColumnSearchToggle class -- this object represents a checkbox DOM element
 *  whose state should be tied to whether or not a certain column in a
 *  table should be searched when the table is filtered.
 *
 * @param  column  The column index to be toggled
 * @param  elemID  The DOM element (a checkbox) to reference
 * @return ColumnSearchToggle A column search object
 */
function /* class */ ColumnSearchToggle(column, elemID) {
  this.column = column;
  this.elem = document.getElementById(elemID);
  if (this.elem === null) {
    console.log('Error: ' + elemID + ' does not exist');
  }
}

/**
 * Get an array of column indices that are allowed to be filtered by their
 * toggles.
 *
 * @param  columnToggles An array of ColumnSearchToggles.
 * @return Array         An array of indices (as integers).
 */
function _getColumnArray(columnToggles) {
  var columns = [];
  for (var t in columnToggles) {
    var toggle = columnToggles[t];

    if (toggle.elem.checked) {
      columns.push(toggle.column);
    }
  }

  return columns;
}

/**
 *  Table class -- a DOM table to be filtered.
 *
 * @param  elem          A DOM reference to the table
 * @param  firstRow      The first (non-header) row to be filtered
 * @param  columnSources An array of filterable columns
 * @return Table         An object that will be affected by filterTables()
 */
function /* class */ Table(elem, firstRow, columnSources) {
  this.elem = elem;
  this.firstRow = firstRow;
  this.columnSources = columnSources;
}

/**
 * Add a DOM table to the list of filterable tables.
 *
 * @param  elem          A DOM reference to the table
 * @param  firstRow      The first (non-header) row to be filtered
 * @param  columnSources An array of filterable columns
 */
function addTable(elem, firstRow, columnSources) {
  var t = new Table(elem, firstRow, columnSources);
  filteredTables.push(t);
}

/**
 * Set a filter on all added tables.
 *
 * @param  filter A regex string by which to filter rows of the added tables
 */
function filterTables(filter) {
  try {
    var regex = new RegExp(filter, 'im');
  } catch (ex) {
    return; // When the supplied regex is invalid, just abort.
  }

  for (var t in filteredTables) {
    var tbl = filteredTables[t];
    var columns = _getColumnArray(tbl.columnSources);

    var rows = tbl.elem.rows;
    for (var i = tbl.firstRow; i < rows.length; i++) {
      _filterRow(rows[i], columns, regex);
    }
  }
}

/**
 * Filter an individual row of a table, showing or hiding it as it matches/does
 * not match the filter.
 *
 * @param  row     The DOM table row.
 * @param  columns An array of columns to be searched.
 * @param  regex   A RegExp object to be applied to search columns.
 */
function _filterRow(row, columns, regex) {
  if (columns === null) row.style.display = ''; // Show the row

  var match = false;
  for (var c in columns) {
    var col_num = columns[c];

    if (regex.test(row.cells[col_num].innerHTML)) {
      match = true;
      break;
    }
  }

  row.style.display = (match ? '' : 'none');
}

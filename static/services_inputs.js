/**
 * Guided service inputs: fetch field help JSON and render under each field.
 * Lookup / unique-value help supports click-to-fill when data-field-key is set.
 */
(function () {
  function escapeHtml(s) {
    var d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function fillFieldFromHelp(fieldKey, value) {
    if (!fieldKey) return;
    var el =
      document.getElementById("si_" + fieldKey) ||
      document.getElementById("field-" + fieldKey) ||
      document.getElementById("tw_field_" + fieldKey);
    if (!el) return;
    el.value = value === null || value === undefined ? "" : String(value);
    try {
      el.dispatchEvent(new Event("input", { bubbles: true }));
    } catch (e) {
      /* IE / old engines */
    }
  }

  function resolveFillColumn(keys, fillColumn) {
    if (!keys || !keys.length) return keys[0];
    var want = (fillColumn || "").trim().toLowerCase();
    if (!want) return keys[0];
    for (var i = 0; i < keys.length; i++) {
      if (String(keys[i]).toLowerCase() === want) return keys[i];
    }
    return keys[0];
  }

  function renderUniqueValues(container, values, fieldKey) {
    if (!values || !values.length) {
      container.innerHTML =
        '<span class="page-description">No sample values in mock DB for this column.</span>';
      return;
    }
    container.innerHTML = "";
    if (fieldKey) {
      var noteU = document.createElement("p");
      noteU.className = "page-description help-click-hint";
      noteU.textContent = "Click a value to fill the field.";
      container.appendChild(noteU);
    }
    var ul = document.createElement("ul");
    ul.className = "help-values-list help-values-list--clickable";
    values.forEach(function (v) {
      var li = document.createElement("li");
      li.className = "help-value-item";
      li.textContent = v === null || v === undefined ? "" : String(v);
      if (fieldKey) {
        li.style.cursor = "pointer";
        li.setAttribute("role", "button");
        li.addEventListener("click", function () {
          fillFieldFromHelp(fieldKey, v);
        });
      }
      ul.appendChild(li);
    });
    container.appendChild(ul);
  }

  function renderLookupRows(container, rows, fieldKey, fillColumn) {
    if (!rows || !rows.length) {
      container.innerHTML =
        '<span class="page-description">No rows in mock DB for this lookup.</span>';
      return;
    }
    var table = document.createElement("table");
    table.className = "help-lookup-table help-lookup-table--clickable";
    var thead = document.createElement("thead");
    var trh = document.createElement("tr");
    var keys = Object.keys(rows[0]);
    var colPick = resolveFillColumn(keys, fillColumn);
    keys.forEach(function (k) {
      var th = document.createElement("th");
      th.textContent = k;
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    table.appendChild(thead);
    var tbody = document.createElement("tbody");
    rows.forEach(function (row) {
      var tr = document.createElement("tr");
      tr.className = "help-lookup-row";
      if (fieldKey) {
        tr.style.cursor = "pointer";
        tr.setAttribute("title", "Click to fill the field");
      }
      keys.forEach(function (k) {
        var td = document.createElement("td");
        var val = row[k];
        td.textContent = val === null || val === undefined ? "" : String(val);
        tr.appendChild(td);
      });
      if (fieldKey) {
        tr.addEventListener("click", function () {
          var pick = row[colPick];
          fillFieldFromHelp(fieldKey, pick);
        });
      }
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    container.innerHTML = "";
    if (fieldKey) {
      var note = document.createElement("p");
      note.className = "page-description help-click-hint";
      note.textContent =
        "Click a row to fill this field from column \"" + colPick + "\".";
      container.appendChild(note);
    }
    container.appendChild(table);
  }

  function bindHelpButtons() {
    document.querySelectorAll(".services-help-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var url = btn.getAttribute("data-help-url");
        var tid = btn.getAttribute("data-help-target");
        var fieldKey = btn.getAttribute("data-field-key") || "";
        var out = tid ? document.getElementById(tid) : null;
        if (!url || !out) return;
        var isOpen = out.getAttribute("data-open") === "1";
        if (isOpen) {
          out.style.display = "none";
          out.setAttribute("data-open", "0");
          return;
        }
        out.style.display = "";
        out.setAttribute("data-open", "1");
        var hasLoaded = out.getAttribute("data-loaded") === "1";
        if (hasLoaded) {
          return;
        }
        out.innerHTML =
          '<span class="page-description">Loading…</span>';
        fetch(url, { credentials: "same-origin" })
          .then(function (r) {
            return r.json().then(function (data) {
              return { ok: r.ok, status: r.status, data: data };
            });
          })
          .then(function (res) {
            if (!res.ok) {
              out.innerHTML =
                '<span class="alert alert-error" style="display:inline-block;margin-top:0.35rem;">' +
                escapeHtml(res.data.error || "Help request failed") +
                "</span>";
              return;
            }
            var d = res.data;
            if (d.mode === "unique_values") {
              renderUniqueValues(out, d.values, fieldKey);
            } else if (d.mode === "lookup_rows") {
              renderLookupRows(out, d.rows, fieldKey, d.fill_column || "");
            } else {
              out.innerHTML =
                '<span class="page-description">Unexpected help response.</span>';
            }
            out.setAttribute("data-loaded", "1");
          })
          .catch(function () {
            out.innerHTML =
              '<span class="alert alert-error" style="display:inline-block;margin-top:0.35rem;">Network error</span>';
          });
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindHelpButtons);
  } else {
    bindHelpButtons();
  }
})();

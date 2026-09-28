function decodeCompositeKey(rawValue, expectedColumnCount, tableName) {
  const str = String(rawValue);

  if (str.startsWith("[")) {
    try {
      const parsed = JSON.parse(str);
      if (Array.isArray(parsed)) {
        if (parsed.length !== expectedColumnCount) {
          throw new Error(
            `primary_key_val JSON array has ${parsed.length} element(s), expected ${expectedColumnCount} for table '${tableName}'`,
          );
        }
        return parsed.map((v) =>
          v === null || v === undefined ? null : String(v),
        );
      }
    } catch (err) {
      throw new Error(
        `Failed to decode primary_key_val for table '${tableName}': ${err.message}`,
      );
    }
  }

  const parts = str.split("-");
  if (parts.length !== expectedColumnCount) {
    throw new Error(
      `Legacy dash-delimited primary_key_val "${str}" split into ${parts.length} part(s), expected ${expectedColumnCount} for table '${tableName}'. ` +
        `This PK value likely contains a "-" (negative number or UUID column) and can't be safely decoded in the legacy format — ` +
        `re-run the trigger setup to switch this table to the JSON-array encoding.`,
    );
  }
  return parts;
}

module.exports = { decodeCompositeKey };

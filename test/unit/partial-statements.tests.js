'use strict';

var assume = require('assume');
var With = require('../../lib/statement-builder/partial-statements/with');

describe('Partial Statements', function () {

  describe('with', function () {

    it('should return a with partial statement given appropriate options', function () {
      var w = new With({
        compaction: {
          cool: 'things can happen',
          when: 'you use proper compaction'
        },
        gcGraceSeconds: 9600
      });

      assume(w.cql).to.be.a('string');
      assume(w.cql).to.contain('compaction');
      assume(w.cql).to.contain('gc_grace_seconds');
      assume(w.error).is.falsey();
    });

    it('should return an error on the partial statement when it cannot process the given options', function () {
      var w = new With({
        what: new RegExp()
      });

      assume(w.error).is.instanceof(Error);
    });
  });
});

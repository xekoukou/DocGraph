package platanos.docGraphDB;


import java.util.List;

import com.google.protobuf.ByteString;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.exceptions.HectorException;

class DocMetadata {

	protected ColumnFamilyTemplate<Long, Composite> template;
	protected Keyspace ksp;
	protected Keyspace rksp;
	protected ThriftColumnFamilyTemplate<Long, Long> rtemplate;
	protected Keyspace sksp;
	protected ThriftColumnFamilyTemplate<byte[], Composite> stemplate;

	DocMetadata(Keyspace ksp, Keyspace rksp, Keyspace sksp) {
		template = new ThriftColumnFamilyTemplate<Long, Composite>(ksp,
				"DocMetadata", LongSerializer.get(),
				new CompositeSerializer());
		rtemplate = new ThriftColumnFamilyTemplate<Long, Long>(rksp,
				"LuceneUIDs", LongSerializer.get(),
				LongSerializer.get());
		stemplate = new ThriftColumnFamilyTemplate<byte[], Composite>(sksp,
				"GraphUIDs", BytesArraySerializer.get(),
				new CompositeSerializer());
		
		this.ksp=ksp;
		this.rksp=rksp;
		this.sksp=sksp;
	}

	protected boolean addSha3(Long key, byte[] position, byte[] data) {
		try {

			Composite comp = new Composite();
			comp.add(0, position);
			comp.add(1, "sha3");
			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);

			updater.setByteArray(comp, data);
			template.update(updater);
			
			// sha3 to graphUID
			ColumnFamilyUpdater<byte[], Composite> supdater = stemplate.createUpdater(data);
			Composite scomp = new Composite();
			scomp.add(0, key);
			scomp.add(1,position);
			byte[] zero = new byte[0];
			supdater.setByteArray(scomp, zero);
			stemplate.update(supdater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

	protected boolean addLuceneUID(Long key, byte[] position, Long luceneUID) {
		try {

			Composite comp = new Composite();
			comp.add(0, position);
			comp.add(1, "luceneUID");
			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);

			updater.setLong(comp, luceneUID);
			template.update(updater);
			
			
			comp = new Composite();
			comp.add(0, key);
			comp.add(1, position);
			
			ColumnFamilyUpdater<Long, Long> rupdater = rtemplate.createUpdater((Long)(luceneUID/100000));
			rupdater.setValue(luceneUID, comp, new CompositeSerializer());
			rtemplate.update(rupdater);
			

			return true;
		} catch (HectorException e) {
			return false;
		}
	}
	
	protected boolean addTime(Long key, byte[] position, int data) {
		try {

			Composite comp = new Composite();
			comp.add(0, position);
			comp.add(1, "time");
			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);

			updater.setInteger(comp, data);
			template.update(updater);
			
			return true;
			} catch (HectorException e) {
				return false;
			}
		}
		
	protected boolean addAuthor(Long key, byte[] position, byte[] data) {
		try {

			Composite comp = new Composite();
			comp.add(0, position);
			comp.add(1, "author");
			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);

			updater.setByteArray(comp, data);
			template.update(updater);
			
			return true;
			} catch (HectorException e) {
				return false;
			}
		}
		
	protected boolean addLocation(Long key, byte[] position, byte[] data) {
		try {

			Composite comp = new Composite();
			comp.add(0, position);
			comp.add(1, "location");
			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);

			updater.setByteArray(comp, data);
			template.update(updater);
			
			return true;
			} catch (HectorException e) {
				return false;
			}
		}
		
	protected boolean addSeller(Long key, byte[] position, byte[] data) {
		try {

			Composite comp = new Composite();
			comp.add(0, position);
			comp.add(1, "seller");
			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);

			updater.setByteArray(comp, data);
			template.update(updater);
			
			return true;
			} catch (HectorException e) {
				return false;
			}
		}
		
	protected boolean addPeriod(Long key, byte[] position, byte[] data) {
		try {

			Composite comp = new Composite();
			comp.add(0, position);
			comp.add(1, "period");
			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);

			updater.setByteArray(comp, data);
			template.update(updater);
			
			return true;
			} catch (HectorException e) {
				return false;
			}
		}
	protected boolean addSha3Contract(Long key, byte[] position, byte[] data) {
		try {

			Composite comp = new Composite();
			comp.add(0, position);
			comp.add(1, "sha3Contract");
			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);

			updater.setByteArray(comp, data);
			template.update(updater);
			
			return true;
			} catch (HectorException e) {
				return false;
			}
		}
		
	protected boolean addPrevSha3(Long key, byte[] position, List<ByteString> data) {
		try {

			
			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);
			Composite comp;

switch(data.size()){
case 2:	
			comp = new Composite();
			comp.add(0, position);
			comp.add(1, "prevSha3-2");
			updater.setByteArray(comp, data.get(1).toByteArray());
			
case 1:		comp = new Composite();
			comp.add(0, position);
			comp.add(1, "prevSha3-1");
			updater.setByteArray(comp, data.get(0).toByteArray());
			template.update(updater);
}

			return true;
			} catch (HectorException e) {
				return false;
			}
		}
	
	
		
}
